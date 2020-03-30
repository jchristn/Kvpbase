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

using Kvpbase.StorageServer.Classes.Managers;
using Kvpbase.StorageServer.Classes;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer.Classes
{
    internal class ContainerClient : IDisposable
    {
        internal Container Container
        {
            get
            {
                return _Container;
            } 
        } 
         
        private bool _Disposed = false; 
        private Settings _Settings;
        private LoggingModule _Logging;
        private DatabaseManager _Database;
        private Container _Container;
        private static string _Header = null;

        private DiskDriver _DiskHandler = null;

        private readonly object _LockKey;
        private List<string> _LockedKeys;
         
        internal ContainerClient(Settings settings, LoggingModule logging, DatabaseManager database, Container container)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (database == null) throw new ArgumentNullException(nameof(database)); 
            if (container == null) throw new ArgumentNullException(nameof(container));

            _Settings = settings;
            _Logging = logging;
            _Database = database; 
            _Container = container;
            _Header = "[Kvpbase.Container " + _Container.GUID + "] ";

            _LockKey = new object();
            _LockedKeys = new List<string>(); 

            _DiskHandler = new DiskDriver(_Logging);

            InitializeDirectory();
        }
         
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        internal void Destroy()
        {
            string header = _Header + "Destroy " + _Container.UserGUID + "/" + _Container.Name + " ";
            _Logging.Alert(header + "deletion requested");

            ContainerMetadata md = null;
            ErrorCode error = ErrorCode.None;

            while (true)
            {
                md = Enumerate(null, null, null, null);
                if (md == null || md.Objects == null || md.Objects.Count == 0)
                {
                    _Logging.Debug(header + "end of object enumeration");
                    break;
                }
                else
                {
                    _Logging.Debug(header + "retrieved " + md.Objects.Count + " object(s) for deletion");
                }

                foreach (ObjectMetadata obj in md.Objects)
                {
                    RemoveObject(obj.ObjectKey, out error);
                }
            }
             
            try
            {
                _Logging.Debug(header + "deleting objects directory " + _Container.ObjectsDirectory);
                Directory.Delete(_Container.ObjectsDirectory);
            }
            catch (Exception)
            {
                _Logging.Warn(header + "unable to delete objects directory " + _Container.ObjectsDirectory);
            }

            _Logging.Debug(header + "disposing container client");
            Dispose();  
        }

        internal bool WriteObject(string key, string contentType, byte[] data, List<string> tags, out ErrorCode error)
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

        internal bool WriteObject(string key, string contentType, long contentLength, Stream stream, List<string> tags, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(contentType)) contentType = "application/octet-stream";
            if (contentLength < 0) throw new ArgumentException("Invalid content length.");
            key = key.ToLower();

            string header = _Header + "WriteObject " + _Container.UserGUID + "/" + _Container.Name + "/" + key + " ";

            error = ErrorCode.None;
            bool cleanupRequired = false;
            ObjectMetadata md = null;

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(header + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                if (Exists(key))
                {
                    _Logging.Warn(header + "already exists");
                    error = ErrorCode.AlreadyExists;
                    return false;
                }
                 
                md = new ObjectMetadata(_Container.GUID, key, contentType, contentLength, tags);
                md = _Database.Insert<ObjectMetadata>(md);
                 
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
                    _Logging.Warn(header + "error while writing object: " + error.ToString());
                    cleanupRequired = true; 
                    return false;
                }

                if (!String.IsNullOrEmpty(md5))
                {
                    md.Md5 = md5;
                    _Database.Update<ObjectMetadata>(md);
                }

                _Logging.Debug(header + "successfully wrote object " + md.GUID);
                return success;
            }
            finally
            {
                if (cleanupRequired && md != null)
                {
                    _Database.DeleteByGUID<ObjectMetadata>(md.GUID);
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

        internal bool WriteObject(ObjectMetadata md, byte[] data, out ErrorCode error)
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

        internal bool WriteObject(ObjectMetadata md, Stream stream, out ErrorCode error)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (String.IsNullOrEmpty(md.ObjectKey)) throw new ArgumentException("Key not supplied in metadata.");
            md.ObjectKey = md.ObjectKey.ToLower();
            error = ErrorCode.None;
            string header = _Header + "WriteObject " + _Container.UserGUID + "/" + _Container.Name + "/" + md.ObjectKey + " ";
            bool cleanupRequired = false;

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(md.ObjectKey))
                {
                    _Logging.Warn(header + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(md.ObjectKey);
            }

            try
            { 
                if (Exists(md.ObjectKey))
                {
                    _Logging.Warn(header + "already exists");
                    error = ErrorCode.AlreadyExists;
                    return false;
                }

                md = _Database.Insert<ObjectMetadata>(md);
                 
                string md5 = null;
                bool success = _DiskHandler.Write(Container.ObjectsDirectory + md.GUID, Convert.ToInt64(md.ContentLength), stream, out md5, out error);
                if (!success)
                {
                    _Logging.Warn(header + "error while writing object: " + error.ToString());
                    cleanupRequired = true; 
                    return false;
                }

                if (!String.IsNullOrEmpty(md5))
                {
                    md.Md5 = md5;
                    _Database.Update<ObjectMetadata>(md);
                }
 
                _Logging.Debug(header + "successfully wrote object " + md.GUID);
                return success;
            }
            finally
            {
                if (cleanupRequired)
                {
                    _Database.DeleteByGUID<ObjectMetadata>(md.GUID);
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

        internal bool WriteRangeObject(string key, long position, byte[] data, out ErrorCode error)
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

        internal bool WriteRangeObject(string key, long position, long contentLength, Stream stream, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            error = ErrorCode.None;
            key = key.ToLower();
            string header = _Header + "WriteObjectRange " + _Container.UserGUID + "/" + _Container.Name + "/" + key + " ";
            string guid = null; 

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(header + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            { 
                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(header + "unable to retrieve metadata");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;
                 
                string md5 = null;
                bool success = _DiskHandler.WriteRange(Container.ObjectsDirectory + md.GUID, position, contentLength, stream, out md5, out error);
                if (!success)
                {
                    _Logging.Warn(header + "error while writing object: " + error.ToString());
                    return false;
                }
                 
                if (success && !String.IsNullOrEmpty(md5))
                {
                    md.Md5 = md5;
                    md.LastUpdateUtc = DateTime.Now.ToUniversalTime();
                    long size = 0;

                    if (_DiskHandler.GetObjectSize(Container.ObjectsDirectory + md.GUID, out size, out error))
                    {
                        md.ContentLength = size;
                    }

                    _Database.Update<ObjectMetadata>(md);
                }
                 
                _Logging.Debug(header + "successfully updated object " + md.GUID);
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

        internal bool WriteObjectTags(string key, string tags, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(tags)) throw new ArgumentNullException(nameof(tags));
            key = key.ToLower();
            string header = _Header + "WriteObjectTags " + _Container.UserGUID + "/" + _Container.Name + "/" + key + " ";
            string guid = null; 
             
            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(header + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            { 
                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(header + "object not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;
                md.Tags = Common.CsvToStringList(tags);
                md.LastUpdateUtc = DateTime.Now.ToUniversalTime();
                _Database.Update<ObjectMetadata>(md);
                  
                _Logging.Debug(header + "successfully updated tags");
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

        internal void WriteContainerKeyValuePairs(Dictionary<string, string> vals)
        {
            string header = _Header + "WriteContainerKeyValuePairs " + _Container.UserGUID + "/" + _Container.Name + " ";
            _Logging.Debug(header + "rewriting metadata");

            Expression e = new Expression("containerguid", Operators.Equals, _Container.GUID);
            _Database.DeleteByFilter<ContainerKeyValuePair>(e);

            if (vals != null && vals.Count > 0)
            {
                foreach (KeyValuePair<string, string> kvp in vals)
                {
                    ContainerKeyValuePair curr = new ContainerKeyValuePair(_Container.GUID, kvp.Key, kvp.Value);
                    _Database.Insert<ContainerKeyValuePair>(curr);
                }
            }

            return;
        }

        internal bool WriteObjectKeyValuePairs(string key, Dictionary<string, string> vals, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            key = key.ToLower();

            Expression e = new Expression("containerguid", Operators.Equals, _Container.GUID);
            e.PrependAnd("objectkey", Operators.Equals, key);
            ObjectMetadata md = _Database.SelectByFilter<ObjectMetadata>(e, "ORDER BY id DESC");
            if (md == null) return false;

            string guid = md.GUID;
            string header = _Header + "WriteObjectKeyValuePairs " + _Container.UserGUID + "/" + _Container.Name + "/" + key + " ";

            e = new Expression("objectguid", Operators.Equals, md.GUID);
            e.PrependAnd("containerguid", Operators.Equals, _Container.GUID);
            _Database.DeleteByFilter<ObjectKeyValuePair>(e);

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(header + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {   
                if (vals != null && vals.Count > 0)
                {
                    foreach (KeyValuePair<string, string> kvp in vals)
                    {
                        ObjectKeyValuePair curr = new ObjectKeyValuePair(_Container.GUID, md.GUID, kvp.Key, kvp.Value);
                        _Database.Insert<ObjectKeyValuePair>(curr);
                    }
                }

                md.LastUpdateUtc = DateTime.Now.ToUniversalTime();
                _Database.Update<ObjectMetadata>(md);
                 
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

        internal bool RemoveObject(string key, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key)); 
            string guid = null;
            key = key.ToLower();
            string header = _Header + "RemoveObject " + _Container.UserGUID + "/" + _Container.Name + "/" + key + " ";
            
            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(header + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            { 
                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(header + "object not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;
                 
                _Logging.Debug(header + "deleting object");
                _Database.Delete<ObjectMetadata>(md); 

                return _DiskHandler.Delete(Container.ObjectsDirectory + md.GUID, out error); 
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

        internal Dictionary<string, string> ReadContainerKeyValues()
        {
            Dictionary<string, string> vals = new Dictionary<string, string>();

            Expression e = new Expression("containerguid", Operators.Equals, _Container.GUID);
            List<ContainerKeyValuePair> kvps = _Database.SelectMany<ContainerKeyValuePair>(null, null, e, "ORDER BY id DESC");
            if (kvps != null && kvps.Count > 0)
            {
                foreach (ContainerKeyValuePair curr in kvps)
                {
                    vals.Add(curr.MetadataKey, curr.MetadataValue);
                }
            }

            return vals;
        }

        internal bool ReadObjectKeyValues(string key, out Dictionary<string, string> vals, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            error = ErrorCode.None;
            vals = new Dictionary<string, string>();
            key = key.ToLower();
            string header = _Header + "ReadObjectKeyValues " + _Container.UserGUID + "/" + _Container.Name + "/" + key + " "; 

            ObjectMetadata md = null;
            if (!ReadObjectMetadata(key, out md))
            {
                _Logging.Warn(header + "object not found");
                error = ErrorCode.NotFound;
                return false;
            }

            Expression e = new Expression("objectguid", Operators.Equals, md.GUID);
            e.PrependAnd("containerguid", Operators.Equals, _Container.GUID);

            List<ObjectKeyValuePair> kvps = _Database.SelectMany<ObjectKeyValuePair>(null, null, e, "ORDER BY id DESC");
            if (kvps != null && kvps.Count > 0)
            {
                foreach (ObjectKeyValuePair curr in kvps)
                {
                    vals.Add(curr.MetadataKey, curr.MetadataValue);
                }
            }

            return true;
        }

        internal bool ReadObjectMetadata(string key, out ObjectMetadata metadata)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key)); 
            key = key.ToLower(); 
            metadata = null; 
            Expression e = new Expression("objectkey", Operators.Equals, key);
            e.PrependAnd("containerguid", Operators.Equals, _Container.GUID);
            metadata = _Database.SelectByFilter<ObjectMetadata>(e, "ORDER BY id DESC"); 
            if (metadata == null) return false;
            return true;
        }

        internal bool ReadObject(string key, out string contentType, out long contentLength, out Stream stream, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            stream = null;
            contentType = null;
            contentLength = 0;
            string guid = null; 
            key = key.ToLower();
            string header = _Header + "ReadObject " + _Container.UserGUID + "/" + _Container.Name + "/" + key + " ";

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(header + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            { 
                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(header + "object not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;
                contentType = md.ContentType;
                 
                bool success = _DiskHandler.Read(Container.ObjectsDirectory + md.GUID, out contentLength, out stream, out error);
                if (success)
                {
                    md.LastAccessUtc = DateTime.Now.ToUniversalTime();
                    _Database.Update<ObjectMetadata>(md);
                }

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

        internal bool ReadRangeObject(string key, long startRange, int numBytes, out string contentType, out Stream stream, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            stream = null;
            contentType = null; 
            string guid = null;
            key = key.ToLower();
            string header = _Header + "ReadRangeObject " + _Container.UserGUID + "/" + _Container.Name + "/" + key + " ";

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    _Logging.Warn(header + "unable to lock key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            { 
                ObjectMetadata md = null;
                if (!ReadObjectMetadata(key, out md))
                {
                    _Logging.Warn(header + "object not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                guid = md.GUID;
                contentType = md.ContentType;
                 
                if (startRange >= md.ContentLength)
                {
                    _Logging.Warn(header + "request out of range: start " + startRange + " bytes " + numBytes);
                    error = ErrorCode.OutOfRange;
                    return false;
                }

                if (startRange + numBytes > md.ContentLength)
                {
                    numBytes = (int)(Convert.ToInt64(md.ContentLength) - startRange);
                }
                 
                bool success = _DiskHandler.ReadRange(Container.ObjectsDirectory + md.GUID, startRange, numBytes, out stream, out error);
                
                if (success)
                {
                    md.LastAccessUtc = DateTime.Now.ToUniversalTime();
                    _Database.Update<ObjectMetadata>(md);
                }

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

        internal bool Exists(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key)); 
            key = key.ToLower(); 
            ObjectMetadata md = null;
            if (!ReadObjectMetadata(key, out md)) return false;
            return _DiskHandler.Exists(Container.ObjectsDirectory + md.GUID);
        }

        internal bool RenameObject(string originalKey, string updatedKey, out ErrorCode error)
        {
            error = ErrorCode.None; 
            if (String.IsNullOrEmpty(originalKey)) throw new ArgumentNullException(nameof(originalKey));
            if (String.IsNullOrEmpty(updatedKey)) throw new ArgumentNullException(nameof(updatedKey));

            originalKey = originalKey.ToLower();
            updatedKey = updatedKey.ToLower();
            string header = _Header + "RenameObject " + _Container.UserGUID + "/" + _Container.Name + "/" + originalKey + " ";

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(originalKey))
                {
                    _Logging.Warn(header + "unable to lock original key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(originalKey);

                if (_LockedKeys.Contains(updatedKey))
                {
                    _Logging.Warn(header + "unable to lock updated key");
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(updatedKey);
            }

            try
            {
                ObjectMetadata originalMd = null;
                if (!ReadObjectMetadata(originalKey, out originalMd))
                {
                    _Logging.Warn(header + "object for original key not found");
                    error = ErrorCode.NotFound;
                    return false;
                }

                ObjectMetadata updatedMd = null;
                if (ReadObjectMetadata(updatedKey, out updatedMd))
                {
                    _Logging.Warn(header + "object for updated key exists");
                    error = ErrorCode.AlreadyExists;
                    return false;
                }

                originalMd.ObjectKey = updatedKey;
                originalMd.LastAccessUtc = DateTime.Now.ToUniversalTime();
                _Database.Update<ObjectMetadata>(originalMd);

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

        internal long ObjectCount()
        {
            string query = null;

            switch (_Settings.Database.Type)
            {
                case DbTypes.Sqlite:
                    query = "SELECT COUNT(*) AS `NumObjects` FROM `" + DatabaseManager.OBJECTS_TABLE + "`";
                    break;
                case DbTypes.MsSql:
                    query = "SELECT COUNT(*) AS [NumObjects] FROM [" + DatabaseManager.OBJECTS_TABLE + "]";
                    break;
                case DbTypes.MySql:
                    query = "SELECT COUNT(*) AS `NumObjects` FROM `" + DatabaseManager.OBJECTS_TABLE + "`";
                    break;
                case DbTypes.PgSql:
                    query = "SELECT COUNT(*) AS \"NumObjects\" FROM \"" + DatabaseManager.OBJECTS_TABLE + "\"";
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

        internal long BytesConsumed()
        {
            string query = null;

            switch (_Settings.Database.Type)
            {
                case DbTypes.Sqlite:
                    query = "SELECT SUM(`ContentLength`) AS `Bytes` FROM `" + DatabaseManager.OBJECTS_TABLE + "`";
                    break;
                case DbTypes.MsSql:
                    query = "SELECT SUM([ContentLength]) AS [Bytes] FROM [" + DatabaseManager.OBJECTS_TABLE + "]";
                    break;
                case DbTypes.MySql:
                    query = "SELECT SUM(`ContentLength`) AS `Bytes` FROM `" + DatabaseManager.OBJECTS_TABLE + "`";
                    break;
                case DbTypes.PgSql:
                    query = "SELECT SUM(\"ContentLength\") AS \"Bytes\" FROM \"" + DatabaseManager.OBJECTS_TABLE + "\"";
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

        internal ContainerMetadata GetStatistics()
        {
            ContainerMetadata meta = new ContainerMetadata();
            meta.UserGUID = Container.UserGUID;
            meta.ContainerName = Container.Name;
            meta.ContainerGUID = Container.GUID;

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

        internal ContainerMetadata Enumerate(int? indexStart, int? maxResults, EnumerationFilter filter, string orderByClause)
        {
            string query = null;

            switch (_Settings.Database.Type)
            {
                case DbTypes.Sqlite:
                    query = EnumerateQuerySqlite(indexStart, maxResults, filter, orderByClause);
                    break;
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
            meta.UserGUID = Container.UserGUID;
            meta.ContainerName = Container.Name;
            meta.ContainerGUID = Container.GUID;

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

        internal void AddAuditLogEntry(string objectGuid, AuditLogEntryType action, string metadata, bool force)
        {
            if (!force && !Container.EnableAuditLogging) return;
            AuditLogEntry log = new AuditLogEntry(_Container.GUID, objectGuid, action, metadata);
            _Database.Insert<AuditLogEntry>(log);
        }

        internal List<AuditLogEntry> GetAuditLogEntries(string key, string action, int? maxResults, int? index, DateTime? createdBefore, DateTime? createdAfter)
        {
            if (!String.IsNullOrEmpty(key)) key = key.ToLower();

            Expression e = new Expression("id", Operators.GreaterThan, 0);
            
            ObjectMetadata md = null;
            if (!String.IsNullOrEmpty(key))
            {
                if (!ReadObjectMetadata(key, out md)) return null;
                e.PrependAnd("objectguid", Operators.Equals, md.GUID);
            }

            if (!String.IsNullOrEmpty(action)) e.PrependAnd("action", Operators.Equals, action);
            if (createdBefore != null) e.PrependAnd("createdutc", Operators.LessThan, Convert.ToDateTime(createdBefore));
            if (createdAfter != null) e.PrependAnd("createdutc", Operators.GreaterThan, Convert.ToDateTime(createdAfter));

            return _Database.SelectMany<AuditLogEntry>(null, null, e, "ORDER BY id DESC");
        }

        internal void ClearAuditLog()
        {
            string header = _Header + "ClearAuditLog " + _Container.UserGUID + "/" + _Container.Name + " ";
            _Logging.Info(header + "audit log deletion requested");
            Expression e = new Expression("containerguid", Operators.Equals, _Container.GUID);
            _Database.DeleteByFilter<AuditLogEntry>(e);
        }

        internal static void HttpStatusFromErrorCode(ErrorCode error, out int statusCode, out int id)
        {
            statusCode = 0;
            id = 0;

            switch (error)
            {
                case ErrorCode.None:
                case ErrorCode.Success:
                    statusCode = 200;
                    break;

                case ErrorCode.Created:
                    statusCode = 201;
                    break;

                case ErrorCode.OutOfRange:
                    id = 2;
                    statusCode = 416;
                    break;

                case ErrorCode.NotFound:
                    id = 5;
                    statusCode = 404;
                    break;

                case ErrorCode.AlreadyExists:
                    id = 7;
                    statusCode = 409;
                    break;

                case ErrorCode.Locked:
                    id = 8;
                    statusCode = 409;
                    break;

                case ErrorCode.ServerError:
                case ErrorCode.IOError:
                case ErrorCode.PermissionsError:
                case ErrorCode.DiskFull:
                    id = 4;
                    statusCode = 500;
                    break;

                default:
                    throw new ArgumentException("Unknown error code: " + error.ToString());
            }

            return;
        }

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
                        _Database = null;
                    }
                }
                catch (Exception)
                {

                }
            }

            _Disposed = true;
        }
          
        private void InitializeDirectory()
        {
            if (!Container.ObjectsDirectory.EndsWith("/")) Container.ObjectsDirectory += "/";
            if (!Directory.Exists(Container.ObjectsDirectory)) Directory.CreateDirectory(Container.ObjectsDirectory);
        }
           
        private string GetMd5(string guid)
        {
            Expression e = new Expression("guid", Operators.Equals, guid);
            e.PrependAnd("containerguid", Operators.Equals, _Container.GUID);
            ObjectMetadata md = _Database.SelectByFilter<ObjectMetadata>(e, "ORDER BY id DESC");
            if (md != null) return md.Md5;
            else return null;
        }

        private string EnumerateQuerySqlite(int? indexStart, int? maxResults, EnumerationFilter filter, string orderByClause)
        {
            if (String.IsNullOrEmpty(orderByClause)) orderByClause = "ORDER BY `LastUpdateUtc` DESC";
            if (indexStart != null && indexStart < 1) indexStart = 0;
            if (maxResults != null && maxResults < 1) maxResults = null;
            if (maxResults != null && maxResults > 1000) maxResults = 1000;

            string query =
                "SELECT ";

            query +=
                "`o`.* FROM `" + DatabaseManager.OBJECTS_TABLE + "` AS `o` ";

            if (filter != null && filter.KeyValuePairs != null && filter.KeyValuePairs.Count > 0)
            {
                query += "INNER JOIN `" + DatabaseManager.OBJECTS_KVP_TABLE + "` AS `kv` ON `o`.`GUID` = `kv`.`ObjectGUID` ";
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
                            "AND `kv`.`MetadataKey` = '" + Sanitize(curr.Key) + "' ";

                        if (String.IsNullOrEmpty(curr.Value))
                        {
                            query += "AND `kv`.`MetadataValue` IS NULL ";
                        }
                        else
                        {
                            query += "AND `kv`.`MetadataValue` = '" + Sanitize(curr.Value) + "' ";
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
                "[o].* FROM [" + DatabaseManager.OBJECTS_TABLE + "] AS [o] ";

            if (filter != null && filter.KeyValuePairs != null && filter.KeyValuePairs.Count > 0)
            {
                query += "INNER JOIN [" + DatabaseManager.OBJECTS_KVP_TABLE + "] AS [kv] ON [o].[GUID] = [kv].[ObjectGUID] ";
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
                            "AND [kv].[MetadataKey] = '" + Sanitize(curr.Key) + "' ";

                        if (String.IsNullOrEmpty(curr.Value))
                        {
                            query += "AND [kv].[MetadataValue] IS NULL ";
                        }
                        else
                        {
                            query += "AND [kv].[MetadataValue] = '" + Sanitize(curr.Value) + "' ";
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
                "`o`.* FROM `" + DatabaseManager.OBJECTS_TABLE + "` AS `o` ";

            if (filter != null && filter.KeyValuePairs != null && filter.KeyValuePairs.Count > 0)
            {
                query += "INNER JOIN `" + DatabaseManager.OBJECTS_KVP_TABLE + "` AS `kv` ON `o`.`GUID` = `kv`.`ObjectGUID` ";
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
                            "AND `kv`.`MetadataKey` = '" + Sanitize(curr.Key) + "' ";

                        if (String.IsNullOrEmpty(curr.Value))
                        {
                            query += "AND `kv`.`MetadataValue` IS NULL ";
                        }
                        else
                        {
                            query += "AND `kv`.`MetadataValue` = '" + Sanitize(curr.Value) + "' ";
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
                "\"o\".* FROM \"" + DatabaseManager.OBJECTS_TABLE + "\" AS \"o\" ";

            if (filter != null && filter.KeyValuePairs != null && filter.KeyValuePairs.Count > 0)
            {
                query += "INNER JOIN \"" + DatabaseManager.OBJECTS_KVP_TABLE + "\" AS \"kv\" ON \"o\".\"GUID\" = \"kv\".\"ObjectGUID\" ";
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
                            "AND \"kv\".\"MetadataKey\" = '" + Sanitize(curr.Key) + "' ";

                        if (String.IsNullOrEmpty(curr.Value))
                        {
                            query += "AND \"kv\".\"MetadataValue\" IS NULL ";
                        }
                        else
                        {
                            query += "AND \"kv\".\"MetadataValue\" = '" + Sanitize(curr.Value) + "' ";
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
            return _Database.Sanitize(str);
        } 
    }
}
