using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DatabaseWrapper;
using SyslogLogging;

using Kvpbase.Classes.Managers;
using Kvpbase.Classes;

namespace Kvpbase.Containers
{ 
    public class ContainerClient : IDisposable
    {
        #region Public-Members

        public Container Container
        {
            get
            {
                return _Container;
            } 
        } 

        #endregion

        #region Private-Members

        private bool _Disposed = false;

        private Settings _Settings;
        private LoggingModule _Logging;
        private ConfigManager _Config;
        private Container _Container;
        private DatabaseClient _Database; 

        private DiskDriver _DiskHandler = null;

        private readonly object _LockKey;
        private List<string> _LockedKeys;

        private string OBJECTS_TABLE;
        private string CONTAINER_KVP_TABLE;
        private string OBJECTS_KVP_TABLE;
        private string AUDIT_LOG_TABLE;

        #endregion

        #region Constructors-and-Factories
         
        public ContainerClient(Settings settings, LoggingModule logging, ConfigManager config, DatabaseClient database, Container container)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (database == null) throw new ArgumentNullException(nameof(database));
            if (container == null) throw new ArgumentNullException(nameof(container));

            _Settings = settings;
            _Logging = logging;
            _Config = config;
            _Database = database;
            _Container = container;

            _LockKey = new object();
            _LockedKeys = new List<string>();

            OBJECTS_TABLE = ("Kvpbase_" + container.GUID + "_Objects").ToLower();
            CONTAINER_KVP_TABLE = ("Kvpbase_" + container.GUID + "_Container_Kvp").ToLower();
            OBJECTS_KVP_TABLE = ("Kvpbase_" + container.GUID + "_Objects_Kvp").ToLower();
            AUDIT_LOG_TABLE = ("Kvpbase_" + container.GUID + "_Auditlog").ToLower();

            _DiskHandler = new DiskDriver(_Logging);

            InitializeTables();
            InitializeDirectory();
        }

        #endregion

        #region Public-Methods
         
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
         
        public void Destroy()
        {
            string logMessage = "ContainerClient Delete " + _Container.UserGuid + "/" + _Container.Name + " ";
            _Logging.Alert(logMessage + "deletion requested");

            ContainerMetadata md = null;
            ErrorCode error = ErrorCode.None;

            while (true)
            {
                md = Enumerate(null, null, null, null);
                if (md == null || md.Objects == null || md.Objects.Count == 0)
                {
                    _Logging.Debug(logMessage + "end of object enumeration");
                    break;
                }
                else
                {
                    _Logging.Debug(logMessage + "retrieved " + md.Objects.Count + " object(s) for deletion");
                }

                foreach (ObjectMetadata obj in md.Objects)
                {
                    RemoveObject(obj.ObjectKey, out error);
                }
            }

            _Logging.Debug(logMessage + "dropping database tables");
            DropTables();

            _Logging.Debug(logMessage + "disposing container client");
            Dispose();

            try
            {
                _Logging.Debug(logMessage + "deleting objects directory " + _Container.ObjectsDirectory);
                Directory.Delete(_Container.ObjectsDirectory);
            }
            catch (Exception)
            {
                _Logging.Warn(logMessage + "unable to delete objects directory " + _Container.ObjectsDirectory);
            }

            _Logging.Alert(logMessage + "deleted successfully");
            return;
        }
          
        public bool WriteObject(string key, string contentType, byte[] data, List<string> tags, out ErrorCode error)
        {
            error = ErrorCode.None;
            long contentLength = 0;
            if (data != null) contentLength = data.Length;

            MemoryStream stream = null;
            if (data != null && contentLength > 0)
            {
                stream = new MemoryStream(data);
                stream.Seek(0, SeekOrigin.Begin);
            }
            else
            {
                stream = new MemoryStream(new byte[0]);
            }

            return WriteObject(key, contentType, contentLength, stream, tags, out error);
        }
         
        public bool WriteObject(string key, string contentType, long contentLength, Stream stream, List<string> tags, out ErrorCode error)
        {
            error = ErrorCode.None;
            bool cleanupRequired = false;
            string guid = null;
            string logMessage = "WriteObject " + _Container.UserGuid + "/" + _Container.Name + "/" + key + " ";

            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(contentType)) contentType = "application/octet-stream";
            if (contentLength < 0) throw new ArgumentException("Invalid content length.");

            key = key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(logMessage + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                if (Exists(key))
                {
                    _Logging.Warn(logMessage + "already exists");
                    error = ErrorCode.AlreadyExists;
                    return false;
                }

                #region Insert-Metadata

                ObjectMetadata md = new ObjectMetadata(key, contentType, contentLength, tags);
                Dictionary<string, object> insertVals = md.ToInsertDictionary();
                DataTable insertResult = _Database.Insert(OBJECTS_TABLE, insertVals); 
                guid = md.GUID;

                #endregion

                #region Write-Data

                bool success = false;
                string md5 = null;
                 
                if (stream == null || contentLength == 0)
                {
                    success = _DiskHandler.Write(Container.ObjectsDirectory + md.GUID, 0, new MemoryStream(new byte[0]), out md5, out error);
                }
                else
                {
                    success = _DiskHandler.Write(Container.ObjectsDirectory + md.GUID, contentLength, stream, out md5, out error);
                }

                if (!success)
                {
                    _Logging.Warn(logMessage + "error while writing object: " + error.ToString());
                    cleanupRequired = true; 
                    return false;
                }

                if (!String.IsNullOrEmpty(md5)) SetMd5(guid, md5);

                #endregion

                _Logging.Debug(logMessage + "successfully wrote object " + md.GUID);
                return success;
            }
            finally
            {
                if (cleanupRequired)
                {
                    Expression e = new Expression("GUID", Operators.Equals, guid);
                    DataTable cleanupResult = _Database.Delete(OBJECTS_TABLE, e);
                }

                if (!String.IsNullOrEmpty(key))
                {
                    lock (_LockKey)
                    {
                        if (_LockedKeys.Contains(key)) _LockedKeys.Remove(key);
                    }
                }
            }
        }
         
        public bool WriteObject(ObjectMetadata md, byte[] data, out ErrorCode error)
        {
            error = ErrorCode.None;
            md.ContentLength = 0;
            if (data != null) md.ContentLength = data.Length;

            MemoryStream stream = null;
            if (data != null && md.ContentLength > 0)
            {
                stream = new MemoryStream(data);
                stream.Seek(0, SeekOrigin.Begin);
            }
            else
            {
                stream = new MemoryStream(new byte[0]);
            }

            return WriteObject(md, stream, out error);
        }
         
        public bool WriteObject(ObjectMetadata md, Stream stream, out ErrorCode error)
        {
            error = ErrorCode.None;
            bool cleanupRequired = false;
            if (String.IsNullOrEmpty(md.ObjectKey)) throw new ArgumentException("Key not supplied in metadata.");
            md.ObjectKey = md.ObjectKey.ToLower(); 
            string guid = md.GUID; 
            string logMessage = "WriteObject " + _Container.UserGuid + "/" + _Container.Name + "/" + md.ObjectKey + " ";

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(md.ObjectKey))
                {
                    _Logging.Warn(logMessage + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(md.ObjectKey);
            }

            try
            {
                #region Check-for-Duplicate

                if (Exists(md.ObjectKey))
                {
                    _Logging.Warn(logMessage + "already exists");
                    error = ErrorCode.AlreadyExists;
                    return false;
                }

                #endregion

                #region Insert-Metadata
                 
                Dictionary<string, object> insertVals = md.ToInsertDictionary();
                DataTable insertResult = _Database.Insert(OBJECTS_TABLE, insertVals); 

                #endregion

                #region Write-Data

                string md5 = null;
                bool success = _DiskHandler.Write(Container.ObjectsDirectory + md.GUID, Convert.ToInt64(md.ContentLength), stream, out md5, out error);
                if (!success)
                {
                    _Logging.Warn(logMessage + "error while writing object: " + error.ToString());
                    cleanupRequired = true; 
                    return false;
                }

                if (!String.IsNullOrEmpty(md5)) SetMd5(guid, md5);

                #endregion

                _Logging.Debug(logMessage + "successfully wrote object " + md.GUID);
                return success;
            }
            finally
            {
                if (cleanupRequired)
                {
                    Expression e = new Expression("GUID", Operators.Equals, guid);
                    DataTable cleanupResult = _Database.Delete(OBJECTS_TABLE, e);
                }

                if (!String.IsNullOrEmpty(md.ObjectKey))
                {
                    lock (_LockKey)
                    {
                        if (_LockedKeys.Contains(md.ObjectKey)) _LockedKeys.Remove(md.ObjectKey);
                    }
                }
            }
        }
         
        public bool WriteRangeObject(string key, long position, byte[] data, out ErrorCode error)
        {
            error = ErrorCode.None;
            long contentLength = 0;
            if (data != null) contentLength = data.Length;

            MemoryStream stream = null;
            if (data != null && contentLength > 0)
            {
                stream = new MemoryStream(data);
                stream.Seek(0, SeekOrigin.Begin);
            }
            else
            {
                stream = new MemoryStream(new byte[0]);
            }

            return WriteRangeObject(key, position, contentLength, stream, out error);
        }
         
        public bool WriteRangeObject(string key, long position, long contentLength, Stream stream, out ErrorCode error)
        {
            error = ErrorCode.None;
            string guid = null;

            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            key = key.ToLower();
            string logMessage = "WriteObjectRange " + _Container.UserGuid + "/" + _Container.Name + "/" + key + " ";

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(logMessage + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                #region Retrieve-Metadata

                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(logMessage + "unable to retrieve metadata");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;

                #endregion

                #region Write-Data
                 
                string md5 = null;
                bool success = _DiskHandler.WriteRange(Container.ObjectsDirectory + md.GUID, position, contentLength, stream, out md5, out error);
                if (!success)
                {
                    _Logging.Warn(logMessage + "error while writing object: " + error.ToString());
                    return false;
                }

                #endregion

                #region Update-Metadata

                if (success && !String.IsNullOrEmpty(md5))
                {
                    SetMd5(guid, md5);
                    SetLastUpdateUtc(guid, DateTime.Now.ToUniversalTime());
                    long size = 0;

                    if (_DiskHandler.GetObjectSize(Container.ObjectsDirectory + md.GUID, out size, out error))
                    {
                        SetObjectSize(guid, size);
                    }
                }

                #endregion

                _Logging.Debug(logMessage + "successfully updated object " + md.GUID);
                return success;
            }
            finally
            {
                if (!String.IsNullOrEmpty(key))
                {
                    lock (_LockKey)
                    {
                        if (_LockedKeys.Contains(key)) _LockedKeys.Remove(key);
                    }
                }
            }
        }
         
        public bool WriteObjectTags(string key, string tags, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(tags)) throw new ArgumentNullException(nameof(tags));
            string guid = null;
            
            key = key.ToLower();
            string logMessage = "WriteObjectTags " + _Container.UserGuid + "/" + _Container.Name + "/" + key + " ";
             
            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(logMessage + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                #region Retrieve-Metadata

                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(logMessage + "object not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;

                #endregion

                Expression e = new Expression("GUID", Operators.Equals, guid);
                Dictionary<string, object> updateVals = new Dictionary<string, object>();
                updateVals.Add("Tags", tags);
                _Database.Update(OBJECTS_TABLE, updateVals, e);

                SetLastUpdateUtc(guid, DateTime.Now.ToUniversalTime());
                _Logging.Debug(logMessage + "successfully updated tags");
                return true;
            }
            finally
            {
                if (!String.IsNullOrEmpty(key))
                {
                    lock (_LockKey)
                    {
                        if (_LockedKeys.Contains(key)) _LockedKeys.Remove(key);
                    }
                }
            }
        }
         
        public void WriteContainerKeyValuePairs(Dictionary<string, string> vals)
        {
            _Logging.Info("WriteContainerKeyValuePairs " + _Container.UserGuid + "/" + _Container.Name + " rewriting metadata");

            Expression e = new Expression("Id", Operators.GreaterThan, 0);
            _Database.Delete(CONTAINER_KVP_TABLE, e);
            
            if (vals != null && vals.Count > 0)
            {
                foreach (KeyValuePair<string, string> kvp in vals)
                {
                    ContainerKeyValuePair curr = new ContainerKeyValuePair(kvp.Key, kvp.Value);
                    _Database.Insert(CONTAINER_KVP_TABLE, curr.ToInsertDictionary());
                }
            }

            return;
        }
         
        public bool WriteObjectKeyValuePairs(string key, Dictionary<string, string> vals, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            string guid = null;
            key = key.ToLower();
            string logMessage = "WriteObjectKeyValuePairs " + _Container.UserGuid + "/" + _Container.Name + "/" + key + " ";
             
            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(logMessage + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                #region Retrieve-Metadata

                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(logMessage + "object not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;

                #endregion

                #region Delete-Previous-Metadata

                _Logging.Info(logMessage + "rewriting metadata");
                Expression e = new Expression("ObjectKey", Operators.Equals, key);
                _Database.Delete(OBJECTS_KVP_TABLE, e);

                #endregion

                #region Write-New-Metadata

                if (vals != null && vals.Count > 0)
                {
                    foreach (KeyValuePair<string, string> kvp in vals)
                    {
                        ObjectKeyValuePair curr = new ObjectKeyValuePair(key, kvp.Key, kvp.Value);
                        _Database.Insert(OBJECTS_KVP_TABLE, curr.ToInsertDictionary());
                    }
                }

                SetLastUpdateUtc(guid, DateTime.Now.ToUniversalTime());

                #endregion

                return true;
            }
            finally
            {
                if (!String.IsNullOrEmpty(key))
                {
                    lock (_LockKey)
                    {
                        if (_LockedKeys.Contains(key)) _LockedKeys.Remove(key);
                    }
                }
            }
        }
         
        public bool RemoveObject(string key, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            string guid = null;
            key = key.ToLower();
            string logMessage = "RemoveObject " + _Container.UserGuid + "/" + _Container.Name + "/" + key + " ";

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(logMessage + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                #region Retrieve-Metadata

                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(logMessage + "object not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;

                #endregion

                #region Delete

                _Logging.Debug(logMessage + "deleting object");
                Expression e = new Expression("GUID", Operators.Equals, guid);
                DataTable deleteResult = _Database.Delete(OBJECTS_TABLE, e);

                bool success = _DiskHandler.Delete(Container.ObjectsDirectory + md.GUID, out error);
                return success;

                #endregion
            }
            finally
            {
                if (!String.IsNullOrEmpty(key))
                {
                    lock (_LockKey)
                    {
                        if (_LockedKeys.Contains(key)) _LockedKeys.Remove(key);
                    }
                }
            }
        }
         
        public void ReadContainerKeyValues(out Dictionary<string, string> vals)
        {
            vals = new Dictionary<string, string>();

            Expression e = new Expression("Id", Operators.GreaterThan, 0);
            DataTable result = _Database.Select(CONTAINER_KVP_TABLE, null, null, null, e, null);

            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow curr in result.Rows)
                {
                    string key = null;
                    string val = null;

                    if (curr.Table.Columns.Contains("MetaKey") && curr["MetaKey"] != null && curr["MetaKey"] != DBNull.Value)
                    {
                        key = curr["MetaKey"].ToString();

                        if (curr.Table.Columns.Contains("MetaValue") && curr["MetaValue"] != null && curr["MetaValue"] != DBNull.Value)
                        {
                            val = curr["MetaValue"].ToString();
                        }

                        vals.Add(key, val);
                    }
                }
            }

            return;
        }
         
        public bool ReadObjectKeyValues(string key, out Dictionary<string, string> vals, out ErrorCode error)
        {
            error = ErrorCode.None;
            vals = new Dictionary<string, string>();
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            string logMessage = "ReadObjectKeyValues " + _Container.UserGuid + "/" + _Container.Name + "/" + key + " ";

            if (!Exists(key))
            {
                _Logging.Warn(logMessage + "object not found");
                error = ErrorCode.NotFound;
                return false;
            }

            Expression e = new Expression("ObjectKey", Operators.Equals, key);
            DataTable result = _Database.Select(OBJECTS_KVP_TABLE, null, null, null, e, null);

            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow curr in result.Rows)
                {
                    string currKey = null;
                    string val = null;

                    if (curr.Table.Columns.Contains("MetaKey") && curr["MetaKey"] != null && curr["MetaKey"] != DBNull.Value)
                    {
                        currKey = curr["MetaKey"].ToString();

                        if (curr.Table.Columns.Contains("MetaValue") && curr["MetaValue"] != null && curr["MetaValue"] != DBNull.Value)
                        {
                            val = curr["MetaValue"].ToString();
                        }

                        vals.Add(currKey, val);
                    }
                }
            }

            return true;
        }
         
        public bool ReadObjectMetadata(string key, out ObjectMetadata metadata)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = key.ToLower();

            metadata = null;

            Expression e = new Expression("ObjectKey", Operators.Equals, key);
            DataTable result = _Database.Select(OBJECTS_TABLE, null, null, null, e, null);

            if (result == null || result.Rows.Count < 1)
            {
                return false;
            }
            else
            {
                metadata = ObjectMetadata.FromDataRow(result.Rows[0]);
                return true;
            }
        }
          
        public bool ReadObject(string key, out string contentType, out long contentLength, out Stream stream, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            stream = null;
            contentType = null;
            contentLength = 0;
            string guid = null;

            key = key.ToLower();
            string logMessage = "ReadObject " + _Container.UserGuid + "/" + _Container.Name + "/" + key + " ";

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(logMessage + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                #region Retrieve-Metadata

                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(logMessage + "object not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;
                contentType = md.ContentType;

                #endregion

                #region Read

                bool success = _DiskHandler.Read(Container.ObjectsDirectory + md.GUID, out contentLength, out stream, out error);
                if (success) SetLastAccessUtc(guid, DateTime.Now.ToUniversalTime());
                return success;

                #endregion
            }
            finally
            {
                if (!String.IsNullOrEmpty(key))
                {
                    lock (_LockKey)
                    {
                        if (_LockedKeys.Contains(key)) _LockedKeys.Remove(key);
                    }
                }
            }
        }
          
        public bool ReadRangeObject(string key, long startRange, int numBytes, out string contentType, out Stream stream, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            stream = null;
            contentType = null;

            string guid = null;
            key = key.ToLower();
            string logMessage = "ReadRangeObject " + _Container.UserGuid + "/" + _Container.Name + "/" + key + " ";

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(logMessage + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                #region Retrieve-Metadata

                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(logMessage + "object not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;
                contentType = md.ContentType;

                #endregion

                #region Check-Range

                if (startRange >= md.ContentLength)
                {
                    _Logging.Warn(logMessage + "request out of range: start " + startRange + " bytes " + numBytes);
                    error = ErrorCode.OutOfRange;
                    return false;
                }

                if (startRange + numBytes > md.ContentLength)
                {
                    numBytes = (int)(Convert.ToInt64(md.ContentLength) - startRange);
                }

                #endregion

                #region Read

                bool success = _DiskHandler.ReadRange(Container.ObjectsDirectory + md.GUID, startRange, numBytes, out stream, out error);
                if (success) SetLastAccessUtc(guid, DateTime.Now.ToUniversalTime());
                return success;

                #endregion
            }
            finally
            {
                if (!String.IsNullOrEmpty(key))
                {
                    lock (_LockKey)
                    {
                        if (_LockedKeys.Contains(key)) _LockedKeys.Remove(key);
                    }
                }
            }
        }
         
        public bool Exists(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = key.ToLower();

            ObjectMetadata md = null;
            bool metadataExists = ReadObjectMetadata(key, out md); 
            if (!metadataExists) return false;
            bool objectExists = _DiskHandler.Exists(Container.ObjectsDirectory + md.GUID);

            return metadataExists && objectExists;
        }
         
        public bool RenameObject(string originalKey, string updatedKey, out ErrorCode error)
        {
            error = ErrorCode.None;

            if (String.IsNullOrEmpty(originalKey)) throw new ArgumentNullException(nameof(originalKey));
            if (String.IsNullOrEmpty(updatedKey)) throw new ArgumentNullException(nameof(updatedKey));

            originalKey = originalKey.ToLower();
            updatedKey = updatedKey.ToLower();
            string logMessage = "ReadRangeObject " + _Container.UserGuid + "/" + _Container.Name + "/" + originalKey + " to " + updatedKey + " ";

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(originalKey))
                {
                    _Logging.Warn(logMessage + "unable to lock original key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(originalKey);

                if (_LockedKeys.Contains(updatedKey))
                {
                    _Logging.Warn(logMessage + "unable to lock updated key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(updatedKey);
            }

            try
            {
                if (!Exists(originalKey))
                {
                    _Logging.Warn(logMessage + "object for original key not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                if (Exists(updatedKey))
                {
                    _Logging.Warn(logMessage + "object for updated key exists");
                    error = ErrorCode.AlreadyExists;
                    return false;
                }
                 
                Expression e = new Expression("ObjectKey", Operators.Equals, originalKey);
                Dictionary<string, object> updateVals = new Dictionary<string, object>();
                updateVals.Add("ObjectKey", updatedKey);
                updateVals.Add("LastUpdateUtc", DateTime.Now.ToUniversalTime());
                DataTable updateResult = _Database.Update(OBJECTS_TABLE, updateVals, e);

                return true;
            }
            finally
            {
                lock (_LockKey)
                {
                    if (!String.IsNullOrEmpty(originalKey))
                    {
                        if (_LockedKeys.Contains(originalKey)) _LockedKeys.Remove(originalKey);
                    }

                    if (!String.IsNullOrEmpty(updatedKey))
                    {
                        if (_LockedKeys.Contains(updatedKey)) _LockedKeys.Remove(updatedKey);
                    }
                }
            }
        }
         
        public long ObjectCount()
        {
            string query = null;

            switch (_Database.Type)
            {
                case DbTypes.MsSql:
                    query = "SELECT COUNT(*) AS [NumObjects] FROM [" + OBJECTS_TABLE + "]";
                    break;
                case DbTypes.MySql:
                    query = "SELECT COUNT(*) AS `NumObjects` FROM `" + OBJECTS_TABLE + "`";
                    break;
                case DbTypes.PgSql:
                    query = "SELECT COUNT(*) AS \"NumObjects\" FROM \"" + OBJECTS_TABLE + "\"";
                    break;
            }

            DataTable result = _Database.Query(query);
            if (result != null && result.Rows.Count == 1)
            {
                if (result.Rows[0]["NumObjects"] != null && result.Rows[0]["NumObjects"] != DBNull.Value)
                    return Convert.ToInt64(result.Rows[0]["NumObjects"]);
            }
            return 0;
        }
         
        public long BytesConsumed()
        {
            string query = null;

            switch (_Database.Type)
            {
                case DbTypes.MsSql:
                    query = "SELECT SUM([ContentLength]) AS [Bytes] FROM [" + OBJECTS_TABLE + "]";
                    break;
                case DbTypes.MySql:
                    query = "SELECT SUM(`ContentLength`) AS `Bytes` FROM `" + OBJECTS_TABLE + "`";
                    break;
                case DbTypes.PgSql:
                    query = "SELECT SUM(\"ContentLength\") AS \"Bytes\" FROM \"" + OBJECTS_TABLE + "\"";
                    break;
            }
             
            DataTable result = _Database.Query(query);
            if (result != null && result.Rows.Count == 1)
            {
                if (result.Rows[0]["Bytes"] != null && result.Rows[0]["Bytes"] != DBNull.Value)
                    return Convert.ToInt64(result.Rows[0]["Bytes"]);
            }
            return 0;
        }
         
        public ContainerMetadata GetStatistics()
        {
            ContainerMetadata meta = new ContainerMetadata();
            meta.UserGuid = Container.UserGuid;
            meta.ContainerName = Container.Name;
            meta.ContainerGuid = Container.GUID;

            meta.IndexStart = null;
            meta.MaxResults = null;
            meta.Filter = null;
            meta.Objects = null;
            meta.Totals.Objects = ObjectCount();
            meta.Totals.Bytes = BytesConsumed();
            meta.PublicRead = Container.IsPublicRead;
            meta.PublicWrite = Container.IsPublicWrite; 
            return meta;
        }
         
        public ContainerMetadata Enumerate(int? indexStart, int? maxResults, EnumerationFilter filter, string orderByClause)
        {
            string query = null;

            switch (_Database.Type)
            {
                case DbTypes.MsSql:
                    query = EnumerateQueryMssql(indexStart, maxResults, filter, orderByClause);
                    break;
                case DbTypes.MySql:
                    query = EnumerateQueryMysql(indexStart, maxResults, filter, orderByClause);
                    break;
                case DbTypes.PgSql:
                    query = EnumerateQueryPgsql(indexStart, maxResults, filter, orderByClause);
                    break;
            }
             
            DataTable result = _Database.Query(query); 
            List<ObjectMetadata> objects = ObjectMetadata.FromDataTable(result);

            ContainerMetadata meta = new ContainerMetadata();
            meta.UserGuid = Container.UserGuid;
            meta.ContainerName = Container.Name;
            meta.ContainerGuid = Container.GUID;

            meta.IndexStart = indexStart;
            meta.MaxResults = maxResults;
            meta.Filter = filter;

            meta.Totals.Objects = ObjectCount();
            if (objects != null && objects.Count > 0) meta.Displayed.Objects = objects.Count;

            meta.Totals.Bytes = BytesConsumed();
            if (objects != null) meta.Displayed.Bytes = objects.Sum(m => Convert.ToInt64(m.ContentLength));

            meta.PublicRead = Container.IsPublicRead;
            meta.PublicWrite = Container.IsPublicWrite; 
            meta.Objects = objects;
            return meta;
        }
          
        public void AddAuditLogEntry(string key, AuditLogEntryType action, string metadata, bool force)
        {
            if (!force && !Container.EnableAuditLogging) return;

            AuditLogEntry log = new AuditLogEntry(key, action, metadata);
            _Database.Insert(AUDIT_LOG_TABLE, log.ToInsertDictionary());
        }
         
        public List<AuditLogEntry> GetAuditLogEntries(string key, string action, int? maxResults, int? index, DateTime? createdBefore, DateTime? createdAfter)
        {
            if (!String.IsNullOrEmpty(key)) key = key.ToLower();

            Expression e = new Expression("Id", Operators.GreaterThan, 0);
            if (!String.IsNullOrEmpty(key)) e.PrependAnd("ObjectKey", Operators.Equals, key);
            if (!String.IsNullOrEmpty(action)) e.PrependAnd("Action", Operators.Equals, action);
            if (createdBefore != null) e.PrependAnd("CreatedUtc", Operators.LessThan, Convert.ToDateTime(createdBefore));
            if (createdAfter != null) e.PrependAnd("CreatedUtc", Operators.GreaterThan, Convert.ToDateTime(createdAfter));

            DataTable result = _Database.Select(AUDIT_LOG_TABLE, index, maxResults, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                List<AuditLogEntry> ret = new List<AuditLogEntry>();
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(AuditLogEntry.FromDataRow(row));
                }
                return ret;
            }
            else
            {
                return new List<AuditLogEntry>();
            }
        }
         
        public void ClearAuditLog()
        {
            _Logging.Info("ClearAuditLog " + _Container.UserGuid + "/" + _Container.Name + " audit log deletion requested");
            Expression e = new Expression("Id", Operators.GreaterThan, 0);
            _Database.Delete(AUDIT_LOG_TABLE, e);
        }

        #endregion

        #region Private-Methods

        protected virtual void Dispose(bool disposing)
        {
            if (_Disposed)
            {
                return;
            }

            if (disposing)
            {
                try
                {
                    if (_Database != null)
                    {
                        _Database.Dispose();
                        _Database = null;
                    }
                }
                catch (Exception)
                {

                }
            }

            _Disposed = true;
        }
         
        private void InitializeTables()
        {
            List<string> tableNames = _Database.ListTables();
            tableNames = tableNames.ConvertAll(d => d.ToLower());

            if (!tableNames.Contains(OBJECTS_TABLE.ToLower()))
            {
                List<Column> objectColumns = ObjectMetadata.GetTableColumns();
                _Database.CreateTable(OBJECTS_TABLE, objectColumns);
            }

            if (!tableNames.Contains(CONTAINER_KVP_TABLE.ToLower()))
            {
                List<Column> containerKvpColumns = ContainerKeyValuePair.GetTableColumns();
                _Database.CreateTable(CONTAINER_KVP_TABLE, containerKvpColumns);
            }

            if (!tableNames.Contains(OBJECTS_KVP_TABLE.ToLower()))
            {
                List<Column> objectKvpColumns = ObjectKeyValuePair.GetTableColumns();
                _Database.CreateTable(OBJECTS_KVP_TABLE, objectKvpColumns);
            }

            if (!tableNames.Contains(AUDIT_LOG_TABLE.ToLower()))
            {
                List<Column> auditLogColumns = AuditLogEntry.GetTableColumns();
                _Database.CreateTable(AUDIT_LOG_TABLE, auditLogColumns);
            }
        }

        private void InitializeDirectory()
        {
            if (!Container.ObjectsDirectory.EndsWith("/")) Container.ObjectsDirectory += "/";
            if (!Directory.Exists(Container.ObjectsDirectory)) Directory.CreateDirectory(Container.ObjectsDirectory);
        }

        private void DropTables()
        {
            _Database.DropTable(OBJECTS_TABLE);
            _Database.DropTable(CONTAINER_KVP_TABLE);
            _Database.DropTable(OBJECTS_KVP_TABLE);
            _Database.DropTable(AUDIT_LOG_TABLE);
        }

        private void SetLastAccessUtc(string guid, DateTime ts)
        {
            Expression e = new Expression("GUID", Operators.Equals, guid);
            Dictionary<string, object> updateVals = new Dictionary<string, object>();
            updateVals.Add("LastAccessUtc", ts);
            _Database.Update(OBJECTS_TABLE, updateVals, e);
        }

        private void SetLastUpdateUtc(string guid, DateTime ts)
        {
            Expression e = new Expression("GUID", Operators.Equals, guid);
            Dictionary<string, object> updateVals = new Dictionary<string, object>();
            updateVals.Add("LastUpdateUtc", ts);
            _Database.Update(OBJECTS_TABLE, updateVals, e);
        }

        private void SetObjectSize(string guid, long size)
        {
            Expression e = new Expression("GUID", Operators.Equals, guid);
            Dictionary<string, object> updateVals = new Dictionary<string, object>();
            updateVals.Add("ContentLength", size);
            _Database.Update(OBJECTS_TABLE, updateVals, e);
        }

        private void SetMd5(string guid, string md5)
        {
            Expression e = new Expression("GUID", Operators.Equals, guid);
            Dictionary<string, object> updateVals = new Dictionary<string, object>();
            updateVals.Add("Md5", md5);
            DataTable result = _Database.Update(OBJECTS_TABLE, updateVals, e);
        }

        private string GetMd5(string guid)
        {
            Expression e = new Expression("GUID", Operators.Equals, guid);
            DataTable result = _Database.Select(OBJECTS_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0 && result.Columns.Contains("Md5"))
            {
                foreach (DataRow currRow in result.Rows)
                {
                    if (currRow["Md5"] != null && currRow["Md5"] != DBNull.Value)
                    {
                        return currRow["Md5"].ToString();
                    }
                }
            }
            return null;
        }

        private string EnumerateQueryMssql(int? indexStart, int? maxResults, EnumerationFilter filter, string orderByClause)
        {
            if (String.IsNullOrEmpty(orderByClause)) orderByClause = "ORDER BY [LastUpdateUtc] DESC";
            if (indexStart != null && indexStart < 1) indexStart = 0;
            if (maxResults != null && maxResults < 1) maxResults = null;
            if (maxResults != null && maxResults > 1000) maxResults = 1000;
             
            string query =
                "SELECT ";

            if (indexStart == null && maxResults == null)
            {
                query += "TOP 100 ";
            }

            query +=
                "[o].* FROM [" + OBJECTS_TABLE + "] AS [o] ";

            if (filter != null && filter.KeyValuePairs != null && filter.KeyValuePairs.Count > 0)
            {
                query += "INNER JOIN [" + OBJECTS_KVP_TABLE + "] AS [kv] ON [o].[ObjectKey] = [kv].[ObjectKey] ";
            }

            if (filter != null)
            {
                query += "WHERE [o].[Id] > 0 ";

                if (filter.CreatedAfter != null)
                {
                    query += "AND [o].[CreatedUtc] > '" + _Database.Timestamp(Convert.ToDateTime(filter.CreatedAfter)) + "' ";
                }

                if (filter.CreatedBefore != null)
                {
                    query += "AND [o].[CreatedUtc] < '" + _Database.Timestamp(Convert.ToDateTime(filter.CreatedBefore)) + "' ";
                }

                if (filter.UpdatedAfter != null)
                {
                    query += "AND [o].[LastUpdateUtc] > '" + _Database.Timestamp(Convert.ToDateTime(filter.UpdatedAfter)) + "' ";
                }

                if (filter.UpdatedBefore != null)
                {
                    query += "AND [o].[LastUpdateUtc] < '" + _Database.Timestamp(Convert.ToDateTime(filter.UpdatedBefore)) + "' ";
                }

                if (filter.LastAccessAfter != null)
                {
                    query += "AND [o].[LastAccessUtc] > '" + _Database.Timestamp(Convert.ToDateTime(filter.LastAccessAfter)) + "' ";
                }

                if (filter.LastAccessBefore != null)
                {
                    query += "AND [o].[LastAccessUtc] < '" + _Database.Timestamp(Convert.ToDateTime(filter.LastAccessBefore)) + "' ";
                }

                if (!String.IsNullOrEmpty(filter.Prefix))
                {
                    query += "AND [o].[ObjectKey] LIKE '" + Sanitize(filter.Prefix) + "%' ";
                }

                if (!String.IsNullOrEmpty(filter.Md5))
                {
                    query += "AND [o].[Md5] = '" + Sanitize(filter.Md5) + "' ";
                }

                if (filter.SizeMin != null)
                {
                    query += "AND [o].[ContentLength] >= '" + filter.SizeMin + "' ";
                }

                if (filter.SizeMax != null)
                {
                    query += "AND [o].[ContentLength] <= '" + filter.SizeMax + "' ";
                }

                if (filter.Tags != null && filter.Tags.Count > 0)
                {
                    foreach (string currTag in filter.Tags)
                    {
                        query += "AND [o].[Tags] LIKE '%" + currTag + "%' ";
                    }
                }

                if (filter.KeyValuePairs != null && filter.KeyValuePairs.Count > 0)
                {
                    foreach (KeyValuePair<string, string> curr in filter.KeyValuePairs)
                    {
                        if (String.IsNullOrEmpty(curr.Key)) continue;

                        query +=
                            "AND [kv].[MetaKey] = '" + Sanitize(curr.Key) + "' ";

                        if (String.IsNullOrEmpty(curr.Value))
                        {
                            query += "AND [kv].[MetaValue] IS NULL ";
                        }
                        else
                        {
                            query += "AND [kv].[MetaValue] = '" + Sanitize(curr.Value) + "' ";
                        }
                    }
                }
            }

            query += orderByClause + " ";
             
            if (indexStart != null)
            {
                query += "OFFSET " + indexStart + " ROWS ";

                if (maxResults != null)
                {
                    query += "FETCH NEXT " + maxResults + " ROWS ONLY";
                }
            }
            
            return query;
        }

        private string EnumerateQueryMysql(int? indexStart, int? maxResults, EnumerationFilter filter, string orderByClause)
        {
            if (String.IsNullOrEmpty(orderByClause)) orderByClause = "ORDER BY `LastUpdateUtc` DESC";
            if (indexStart != null && indexStart < 1) indexStart = 0;
            if (maxResults != null && maxResults < 1) maxResults = null;
            if (maxResults != null && maxResults > 1000) maxResults = 1000;

            string query =
                "SELECT ";
             
            query +=
                "`o`.* FROM `" + OBJECTS_TABLE + "` AS `o` ";

            if (filter != null && filter.KeyValuePairs != null && filter.KeyValuePairs.Count > 0)
            {
                query += "INNER JOIN `" + OBJECTS_KVP_TABLE + "` AS `kv` ON `o`.`ObjectKey` = `kv`.`ObjectKey` ";
            }

            if (filter != null)
            {
                query += "WHERE `o`.`Id` > 0 ";

                if (filter.CreatedAfter != null)
                {
                    query += "AND `o`.`CreatedUtc` > '" + _Database.Timestamp(Convert.ToDateTime(filter.CreatedAfter)) + "' ";
                }

                if (filter.CreatedBefore != null)
                {
                    query += "AND `o`.`CreatedUtc` < '" + _Database.Timestamp(Convert.ToDateTime(filter.CreatedBefore)) + "' ";
                }

                if (filter.UpdatedAfter != null)
                {
                    query += "AND `o`.`LastUpdateUtc` > '" + _Database.Timestamp(Convert.ToDateTime(filter.UpdatedAfter)) + "' ";
                }

                if (filter.UpdatedBefore != null)
                {
                    query += "AND `o`.`LastUpdateUtc` < '" + _Database.Timestamp(Convert.ToDateTime(filter.UpdatedBefore)) + "' ";
                }

                if (filter.LastAccessAfter != null)
                {
                    query += "AND `o`.`LastAccessUtc` > '" + _Database.Timestamp(Convert.ToDateTime(filter.LastAccessAfter)) + "' ";
                }

                if (filter.LastAccessBefore != null)
                {
                    query += "AND `o`.`LastAccessUtc` < '" + _Database.Timestamp(Convert.ToDateTime(filter.LastAccessBefore)) + "' ";
                }

                if (!String.IsNullOrEmpty(filter.Prefix))
                {
                    query += "AND `o`.`ObjectKey` LIKE '" + Sanitize(filter.Prefix) + "%' ";
                }

                if (!String.IsNullOrEmpty(filter.Md5))
                {
                    query += "AND `o`.`Md5` = '" + Sanitize(filter.Md5) + "' ";
                }

                if (filter.SizeMin != null)
                {
                    query += "AND `o`.`ContentLength` >= '" + filter.SizeMin + "' ";
                }

                if (filter.SizeMax != null)
                {
                    query += "AND `o`.`ContentLength` <= '" + filter.SizeMax + "' ";
                }

                if (filter.Tags != null && filter.Tags.Count > 0)
                {
                    foreach (string currTag in filter.Tags)
                    {
                        query += "AND `o`.`Tags` LIKE '%" + currTag + "%' ";
                    }
                }

                if (filter.KeyValuePairs != null && filter.KeyValuePairs.Count > 0)
                {
                    foreach (KeyValuePair<string, string> curr in filter.KeyValuePairs)
                    {
                        if (String.IsNullOrEmpty(curr.Key)) continue;

                        query +=
                            "AND `kv`.`MetaKey` = '" + Sanitize(curr.Key) + "' ";

                        if (String.IsNullOrEmpty(curr.Value))
                        {
                            query += "AND `kv`.`MetaValue` IS NULL ";
                        }
                        else
                        {
                            query += "AND `kv`.`MetaValue` = '" + Sanitize(curr.Value) + "' ";
                        }
                    }
                }
            }

            query += orderByClause + " ";
             
            if (indexStart == null && maxResults == null)
            {
                query += "LIMIT 100";
            }
            else
            {
                if (indexStart == null && maxResults != null)
                {
                    query += "LIMIT " + maxResults;
                }
                else if (indexStart != null && maxResults != null)
                {
                    query += "LIMIT " + maxResults + " OFFSET " + indexStart;
                }
            } 

            return query;
        }

        private string EnumerateQueryPgsql(int? indexStart, int? maxResults, EnumerationFilter filter, string orderByClause)
        {
            if (String.IsNullOrEmpty(orderByClause)) orderByClause = "ORDER BY \"LastUpdateUtc\" DESC";
            if (indexStart != null && indexStart < 1) indexStart = 0;
            if (maxResults != null && maxResults < 1) maxResults = null;
            if (maxResults != null && maxResults > 1000) maxResults = 1000;

            string query =
                "SELECT ";
             
            query +=
                "\"o\".* FROM \"" + OBJECTS_TABLE + "\" AS \"o\" ";

            if (filter != null && filter.KeyValuePairs != null && filter.KeyValuePairs.Count > 0)
            {
                query += "INNER JOIN \"" + OBJECTS_KVP_TABLE + "\" AS \"kv\" ON \"o\".\"ObjectKey\" = \"kv\".\"ObjectKey\" ";
            }

            if (filter != null)
            {
                query += "WHERE \"o\".\"Id\" > 0 ";

                if (filter.CreatedAfter != null)
                {
                    query += "AND \"o\".\"CreatedUtc\" > '" + _Database.Timestamp(Convert.ToDateTime(filter.CreatedAfter)) + "' ";
                }

                if (filter.CreatedBefore != null)
                {
                    query += "AND \"o\".\"CreatedUtc\" < '" + _Database.Timestamp(Convert.ToDateTime(filter.CreatedBefore)) + "' ";
                }

                if (filter.UpdatedAfter != null)
                {
                    query += "AND \"o\".\"LastUpdateUtc\" > '" + _Database.Timestamp(Convert.ToDateTime(filter.UpdatedAfter)) + "' ";
                }

                if (filter.UpdatedBefore != null)
                {
                    query += "AND \"o\".\"LastUpdateUtc\" < '" + _Database.Timestamp(Convert.ToDateTime(filter.UpdatedBefore)) + "' ";
                }

                if (filter.LastAccessAfter != null)
                {
                    query += "AND \"o\".\"LastAccessUtc\" > '" + _Database.Timestamp(Convert.ToDateTime(filter.LastAccessAfter)) + "' ";
                }

                if (filter.LastAccessBefore != null)
                {
                    query += "AND \"o\".\"LastAccessUtc\" < '" + _Database.Timestamp(Convert.ToDateTime(filter.LastAccessBefore)) + "' ";
                }

                if (!String.IsNullOrEmpty(filter.Prefix))
                {
                    query += "AND \"o\".\"ObjectKey\" LIKE '" + Sanitize(filter.Prefix) + "%' ";
                }

                if (!String.IsNullOrEmpty(filter.Md5))
                {
                    query += "AND \"o\".\"Md5\" = '" + Sanitize(filter.Md5) + "' ";
                }

                if (filter.SizeMin != null)
                {
                    query += "AND \"o\".\"ContentLength\" >= '" + filter.SizeMin + "' ";
                }

                if (filter.SizeMax != null)
                {
                    query += "AND \"o\".\"ContentLength\" <= '" + filter.SizeMax + "' ";
                }

                if (filter.Tags != null && filter.Tags.Count > 0)
                {
                    foreach (string currTag in filter.Tags)
                    {
                        query += "AND \"o\".\"Tags\" LIKE '%" + currTag + "%' ";
                    }
                }

                if (filter.KeyValuePairs != null && filter.KeyValuePairs.Count > 0)
                {
                    foreach (KeyValuePair<string, string> curr in filter.KeyValuePairs)
                    {
                        if (String.IsNullOrEmpty(curr.Key)) continue;

                        query +=
                            "AND \"kv\".\"MetaKey\" = '" + Sanitize(curr.Key) + "' ";

                        if (String.IsNullOrEmpty(curr.Value))
                        {
                            query += "AND \"kv\".\"MetaValue\" IS NULL ";
                        }
                        else
                        {
                            query += "AND \"kv\".\"MetaValue\" = '" + Sanitize(curr.Value) + "' ";
                        }
                    }
                }
            }

            query += orderByClause + " ";
             
            if (indexStart == null && maxResults == null)
            {
                query += "LIMIT 100";
            }
            else
            {
                if (indexStart == null && maxResults != null)
                {
                    query += "LIMIT " + maxResults;
                }
                else if (indexStart != null && maxResults != null)
                {
                    query += "LIMIT " + maxResults + " OFFSET " + indexStart;
                }
            } 

            return query;
        }

        private string Sanitize(string str)
        {
            return _Database.SanitizeString(str);
        }

        #endregion
    }
}
