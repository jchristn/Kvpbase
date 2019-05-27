using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqliteWrapper; 

namespace Kvpbase.Container
{
    /// <summary>
    /// Object container.
    /// </summary>
    public class Container : IDisposable
    {
        #region Public-Members

        /// <summary>
        /// The name of the container.
        /// </summary>
        public string Name { get; private set; }
        
        /// <summary>
        /// Settings for the container.
        /// </summary>
        public ContainerSettings Settings { get; private set; }

        #endregion

        #region Private-Members

        private bool _Disposed = false;

        private string _PropertiesFile;  
        private DatabaseClient _Database;

        private DiskHandler _DiskHandler = null;

        private readonly object _LockPropertiesFile;
        private readonly object _LockKey;
        private List<string> _LockedKeys;

        private static string _TimestampFormat = "yyyy-MM-ddTHH:mm:ss.ffffffZ";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="settings">ContainerSettings.</param>
        public Container(ContainerSettings settings)
        {   
            if (settings == null) throw new ArgumentNullException(nameof(settings));

            Settings = settings;
            Name = Settings.Name;

            if (!Settings.RootDirectory.EndsWith("/")) Settings.RootDirectory += "/";
            if (!Settings.ObjectsDirectory.EndsWith("/")) Settings.ObjectsDirectory += "/";

            if (!Directory.Exists(Settings.RootDirectory)) Directory.CreateDirectory(Settings.RootDirectory);
            if (!Directory.Exists(Settings.ObjectsDirectory)) Directory.CreateDirectory(Settings.ObjectsDirectory);
               
            _Database = new DatabaseClient(Settings.DatabaseFilename, Settings.DatabaseDebug);

            ApplyContainerSettings();
            InitializeContainerDatabase();

            switch (Settings.HandlerType)
            {
                case ObjectHandlerType.Disk:
                    _DiskHandler = new DiskHandler();
                    break;
                default:
                    throw new ArgumentException("Unknown disk handler type in container settings");
            }

            _LockPropertiesFile = new object();
            _LockKey = new object();
            _LockedKeys = new List<string>();

            _PropertiesFile = Settings.RootDirectory + "__Container__.json";
            WritePropertiesFile();
        }

        #endregion

        #region Public-Methods
        
        /// <summary>
        /// Dispose of the Container object.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Apply container settings to the file system, i.e. ensure directories and files are created.
        /// </summary>
        public void ApplyContainerSettings()
        {
            if (!Directory.Exists(Settings.RootDirectory)) Directory.CreateDirectory(Settings.RootDirectory);
            if (!Directory.Exists(Settings.ObjectsDirectory)) Directory.CreateDirectory(Settings.ObjectsDirectory);

            if (Settings.IsPublicRead) CreateEmptyFile(Settings.RootDirectory + "__Pubread__");
            else DeleteFileIfExists(Settings.RootDirectory + "__Pubread__");

            if (Settings.IsPublicWrite) CreateEmptyFile(Settings.RootDirectory + "__Pubwrite__");
            else DeleteFileIfExists(Settings.RootDirectory + "__Pubwrite__");

            if (Settings.EnableAuditLogging) CreateEmptyFile(Settings.RootDirectory + "__AuditLogging__");
            else DeleteFileIfExists(Settings.RootDirectory + "__AuditLogging__");

            switch (Settings.Replication)
            {
                case ReplicationMode.Sync:
                    CreateEmptyFile(Settings.RootDirectory + "__ReplicationSync__");
                    DeleteFileIfExists(Settings.RootDirectory + "__ReplicationAsync__");
                    return;
                case ReplicationMode.Async:
                    CreateEmptyFile(Settings.RootDirectory + "__ReplicationAsync__");
                    DeleteFileIfExists(Settings.RootDirectory + "__ReplicationSync__");
                    return;
                case ReplicationMode.None:
                    DeleteFileIfExists(Settings.RootDirectory + "__ReplicationAsync__");
                    DeleteFileIfExists(Settings.RootDirectory + "__ReplicationSync__");
                    break;
            }
        }

        /// <summary>
        /// Determine if the container is enabled for public read operations.
        /// </summary>
        /// <returns>True if public read is enabled.</returns>
        public bool IsPublicRead()
        { 
            return File.Exists(Settings.RootDirectory + "__Pubread__");
        }

        /// <summary>
        /// Determine if the container is enabled for public write operations.
        /// </summary>
        /// <returns>True if public write is enabled.</returns>
        public bool IsPublicWrite()
        { 
            return File.Exists(Settings.RootDirectory + "__Pubwrite__");
        }

        /// <summary>
        /// Determine if the container is enabled for audit logging.
        /// </summary>
        /// <returns>True if audit logging enabled.</returns>
        public bool IsAuditLogging()
        {
            return File.Exists(Settings.RootDirectory + "__AuditLogging__");
        }

        /// <summary>
        /// Enable public read operations on the container.
        /// </summary>
        public void SetPublicRead()
        { 
            CreateEmptyFile(Settings.RootDirectory + "__Pubread__"); 
            Settings.IsPublicRead = true;
            WritePropertiesFile();
        }

        /// <summary>
        /// Enable public write operations on the container.
        /// </summary>
        public void SetPublicWrite()
        { 
            CreateEmptyFile(Settings.RootDirectory + "__Pubwrite__"); 
            Settings.IsPublicWrite = true;
            WritePropertiesFile();
        }

        /// <summary>
        /// Enable audit logging on the container.
        /// </summary>
        public void SetAuditLogging()
        {
            CreateEmptyFile(Settings.RootDirectory + "__AuditLogging__");
            Settings.EnableAuditLogging = true;
            WritePropertiesFile();
        }

        /// <summary>
        /// Disable public read operations on the container.
        /// </summary>
        public void UnsetPublicRead()
        { 
            DeleteFileIfExists(Settings.RootDirectory + "__Pubread__"); 
            Settings.IsPublicRead = false;
            WritePropertiesFile();
        }

        /// <summary>
        /// Disable public write operations on the container.
        /// </summary>
        public void UnsetPublicWrite()
        { 
            DeleteFileIfExists(Settings.RootDirectory + "__Pubwrite__"); 
            Settings.IsPublicWrite = true;
            WritePropertiesFile();
        }

        /// <summary>
        /// Disable audit logging on the container.
        /// </summary>
        public void UnsetAuditLogging()
        {
            DeleteFileIfExists(Settings.RootDirectory + "__AuditLogging__");
            Settings.EnableAuditLogging = true;
            WritePropertiesFile();
        }

        /// <summary>
        /// Set the replication mode of the container.
        /// </summary>
        /// <param name="mode">Replication mode.</param>
        public void SetReplicationMode(ReplicationMode mode)
        {
            Settings.Replication = mode;

            switch (mode)
            {
                case ReplicationMode.Sync:
                    CreateEmptyFile(Settings.RootDirectory + "__ReplicationSync__");
                    DeleteFileIfExists(Settings.RootDirectory + "__ReplicationAsync__");
                    return;
                case ReplicationMode.Async:
                    CreateEmptyFile(Settings.RootDirectory + "__ReplicationAsync__");
                    DeleteFileIfExists(Settings.RootDirectory + "__ReplicationSync__");
                    return;
                case ReplicationMode.None:
                    DeleteFileIfExists(Settings.RootDirectory + "__ReplicationAsync__");
                    DeleteFileIfExists(Settings.RootDirectory + "__ReplicationSync__");
                    break;
            }
             
            WritePropertiesFile();
            return;
        }

        /// <summary>
        /// Retrieve the replication mode of the container.
        /// </summary> 
        /// <returns>Replication mode.</returns>
        public ReplicationMode GetReplicationMode()
        {
            if (File.Exists(Settings.RootDirectory + "__ReplicationSync__")) return ReplicationMode.Sync;
            if (File.Exists(Settings.RootDirectory + "__ReplicationAsync__")) return ReplicationMode.Async;
            return ReplicationMode.None;
        }
        
        /// <summary>
        /// Write an object to the container.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="contentType">The content type of the object.</param>
        /// <param name="data">The object's data.</param>
        /// <param name="tags">Tags associated with the object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool WriteObject(string key, string contentType, byte[] data, List<string> tags, out ErrorCode error)
        {
            error = ErrorCode.None;
            bool cleanupRequired = false;

            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(contentType)) contentType = "application/octet-stream";

            key = key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                if (Exists(key))
                {
                    error = ErrorCode.AlreadyExists;
                    return false;
                }

                #region Insert-Metadata

                ObjectMetadata md = new ObjectMetadata(key, contentType, data, tags);

                string ts = TimestampUtc(DateTime.Now);
                long len = 0;
                string md5 = null;
                if (data != null)
                {
                    len = data.Length;
                    md5 = Common.Md5(data);
                }

                string insertQuery = ContainerQueries.WriteObject(key, contentType, len, md5, tags, ts);
                DataTable insertResult = _Database.Query(insertQuery);

                #endregion

                #region Write-Data

                bool success = false;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.Write(Settings.ObjectsDirectory + key, data, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                if (!success) cleanupRequired = true;
                return success;

                #endregion
            }
            finally
            {
                if (cleanupRequired)
                {
                    string cleanupQuery = ContainerQueries.RemoveObject(key);
                    DataTable cleanupResult = _Database.Query(cleanupQuery);
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

        /// <summary>
        /// Write an object to the container.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="contentType">The content type of the object.</param>
        /// <param name="contentLength">The content length of the object.</param>
        /// <param name="stream">The stream containing the object data.</param>
        /// <param name="tags">Tags associated with the object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool WriteObject(string key, string contentType, long contentLength, Stream stream, List<string> tags, out ErrorCode error)
        {
            error = ErrorCode.None;
            bool cleanupRequired = false;

            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(contentType)) contentType = "application/octet-stream";
            if (contentLength < 0) throw new ArgumentException("Invalid content length.");
            if (stream == null || !stream.CanRead) throw new ArgumentException("Unable to read from stream.");

            key = key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                if (Exists(key))
                {
                    error = ErrorCode.AlreadyExists;
                    return false;
                }

                #region Insert-Metadata

                string ts = TimestampUtc(DateTime.Now);
                ObjectMetadata md = new ObjectMetadata(key, contentType, contentLength, tags); 
                string insertQuery = ContainerQueries.WriteObject(key, contentType, contentLength, null, tags, ts);
                DataTable insertResult = _Database.Query(insertQuery);

                #endregion

                #region Write-Data

                bool success = false;
                string md5 = null;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.Write(Settings.ObjectsDirectory + key, contentLength, stream, out md5, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                if (!success) cleanupRequired = true;

                #endregion

                #region Update-Metadata-with-MD5

                if (!String.IsNullOrEmpty(md5))
                {
                    string updateQuery = ContainerQueries.SetMd5(key, md5);
                    DataTable updateResult = _Database.Query(updateQuery);
                }

                #endregion

                return success;
            }
            finally
            {
                if (cleanupRequired)
                {
                    string cleanupQuery = ContainerQueries.RemoveObject(key);
                    DataTable cleanupResult = _Database.Query(cleanupQuery);
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

        /// <summary>
        /// Write an object to the container.
        /// </summary>
        /// <param name="md">Object metadata.</param>
        /// <param name="data">The object's data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool WriteObject(ObjectMetadata md, byte[] data, out ErrorCode error)
        {
            error = ErrorCode.None;
            bool cleanupRequired = false;
            if (String.IsNullOrEmpty(md.Key)) throw new ArgumentException("Key not supplied in metadata.");

            md.Key = md.Key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(md.Key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(md.Key);
            }

            try
            {
                #region Check-for-Duplicate

                if (Exists(md.Key))
                {
                    error = ErrorCode.AlreadyExists;
                    return false;
                }

                #endregion

                #region Insert-Metadata

                string insertQuery = ContainerQueries.WriteObject(md);
                DataTable insertResult = _Database.Query(insertQuery);

                #endregion

                #region Write-Data

                bool success = false;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.Write(Settings.ObjectsDirectory + md.Key, data, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                if (!success) cleanupRequired = true;
                return success;

                #endregion
            }
            finally
            {
                if (cleanupRequired)
                {
                    string cleanupQuery = ContainerQueries.RemoveObject(md.Key);
                    DataTable cleanupResult = _Database.Query(cleanupQuery);
                }

                if (!String.IsNullOrEmpty(md.Key))
                {
                    lock (_LockKey)
                    {
                        if (_LockedKeys.Contains(md.Key)) _LockedKeys.Remove(md.Key);
                    }
                }
            }
        }

        /// <summary>
        /// Write an object to the container.
        /// </summary>
        /// <param name="md">Object metadata.</param>
        /// <param name="stream">The stream containing the object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool WriteObject(ObjectMetadata md, Stream stream, out ErrorCode error)
        {
            error = ErrorCode.None;
            bool cleanupRequired = false;
            if (String.IsNullOrEmpty(md.Key)) throw new ArgumentException("Key not supplied in metadata.");
            md.Key = md.Key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(md.Key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(md.Key);
            }

            try
            {
                #region Check-for-Duplicate

                if (Exists(md.Key))
                {
                    error = ErrorCode.AlreadyExists;
                    return false;
                }

                #endregion

                #region Insert-Metadata

                string insertQuery = ContainerQueries.WriteObject(md);
                DataTable insertResult = _Database.Query(insertQuery);

                #endregion

                #region Write-Data

                bool success = false;
                string md5 = null;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.Write(Settings.ObjectsDirectory + md.Key, Convert.ToInt64(md.ContentLength), stream, out md5, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                if (!success) cleanupRequired = true;

                #endregion

                #region Update-Metadata-with-MD5

                if (!String.IsNullOrEmpty(md5))
                {
                    string updateQuery = ContainerQueries.SetMd5(md.Key, md5);
                    DataTable updateResult = _Database.Query(updateQuery);
                }

                #endregion

                return success; 
            }
            finally
            {
                if (cleanupRequired)
                {
                    string cleanupQuery = ContainerQueries.RemoveObject(md.Key);
                    DataTable cleanupResult = _Database.Query(cleanupQuery);
                }

                if (!String.IsNullOrEmpty(md.Key))
                {
                    lock (_LockKey)
                    {
                        if (_LockedKeys.Contains(md.Key)) _LockedKeys.Remove(md.Key);
                    }
                }
            }
        }

        /// <summary>
        /// Write bytes to a specific position in an existing object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="position">The position to which data should be written.</param>
        /// <param name="data">The data to write.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool WriteRangeObject(string key, long position, byte[] data, out ErrorCode error)
        {
            error = ErrorCode.None;
            bool cleanupRequired = false;
            byte[] originalData = null;
            string originalMd5 = null;

            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                if (!Exists(key))
                {
                    error = ErrorCode.NotFound;
                    return false;
                }

                #region Read-Original-Data-and-MD5

                if (data != null && data.Length > 0)
                {
                    if (!_DiskHandler.ReadRange(Settings.ObjectsDirectory + key, position, data.Length, out originalData, out error))
                    {
                        if (error != ErrorCode.OutOfRange)
                        {
                            return false;
                        }
                    }
                }
                
                originalMd5 = GetMd5(key);
                
                #endregion

                #region Write-Data

                bool success = false;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.WriteRange(Settings.ObjectsDirectory + key, position, data, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                #endregion

                #region Update-Metadata

                if (success)
                {
                    #region Update-MD5

                    if (!_DiskHandler.Read(Settings.ObjectsDirectory + key, out data, out error))
                    {
                        cleanupRequired = true;
                        return false;
                    }

                    string updatedMd5 = Common.Md5(data);

                    SetMd5(key, updatedMd5);

                    #endregion

                    #region Update-Timestamp

                    SetLastpdateUtc(key, TimestampUtc(DateTime.Now));

                    #endregion

                    #region Update-Object-Size

                    long size;
                    switch (Settings.HandlerType)
                    {
                        case ObjectHandlerType.Disk:
                            if (_DiskHandler.GetObjectSize(Settings.ObjectsDirectory + key, out size, out error))
                            {
                                SetObjectSize(key, size);
                            }
                            break;
                        default:
                            throw new Exception("Unknown disk handler type in container settings");
                    }

                    #endregion
                }
                 
                #endregion
                 
                return success; 
            }
            finally
            {
                if (cleanupRequired && originalData != null)
                {
                    _DiskHandler.WriteRange(Settings.ObjectsDirectory + key, position, originalData, out error);
                    SetMd5(key, originalMd5);
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

        /// <summary>
        /// Write bytes to a specific position in an existing object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="position">The position to which data should be written.</param>
        /// <param name="contentLength">The content length of the object.</param>
        /// <param name="stream">The stream containing the object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool WriteRangeObject(string key, long position, long contentLength, Stream stream, out ErrorCode error)
        {
            error = ErrorCode.None;  

            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            key = key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                if (!Exists(key))
                {
                    error = ErrorCode.NotFound;
                    return false;
                }
                 
                #region Write-Data

                bool success = false;
                string md5 = null;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.WriteRange(Settings.ObjectsDirectory + key, position, contentLength, stream, out md5, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                #endregion

                #region Update-Metadata

                if (success && !String.IsNullOrEmpty(md5))
                {
                    SetMd5(key, md5);
                    SetLastpdateUtc(key, TimestampUtc(DateTime.Now));
                    long size = 0;
                     
                    switch (Settings.HandlerType)
                    {
                        case ObjectHandlerType.Disk:
                            if (_DiskHandler.GetObjectSize(Settings.ObjectsDirectory + key, out size, out error))
                            {
                                SetObjectSize(key, size);
                            }
                            break;
                        default:
                            throw new Exception("Unknown disk handler type in container settings");
                    } 
                }

                #endregion

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

        /// <summary>
        /// Rewrite the tags for a given object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="tags">The tags to apply to the object, in CSV string format.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool WriteObjectTags(string key, string tags, out ErrorCode error)
        {
            error = ErrorCode.None;

            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(tags)) throw new ArgumentNullException(nameof(tags));

            key = key.ToLower();

            if (!Exists(key))
            {
                error = ErrorCode.NotFound;
                return false;
            }

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                string query = ContainerQueries.SetTags(key, tags);
                DataTable result = _Database.Query(query);
                SetLastpdateUtc(key, TimestampUtc(DateTime.Now)); 
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

        /// <summary>
        /// Remove an object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool RemoveObject(string key, out ErrorCode error)
        { 
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                string query = ContainerQueries.RemoveObject(key);
                DataTable result = _Database.Query(query);

                bool success = false;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.Delete(Settings.ObjectsDirectory + key, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
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

        /// <summary>
        /// Read an object's metadata.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="metadata">The object's metadata.</param>
        /// <returns>True if successful.</returns>
        public bool ReadObjectMetadata(string key, out ObjectMetadata metadata)
        { 
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = key.ToLower();

            metadata = null;
            string query = ContainerQueries.ReadObject(key);
            DataTable result = _Database.Query(query);

            if (result == null || result.Rows.Count < 1)
            { 
                return false;
            }
            else
            {
                metadata = new ObjectMetadata(result.Rows[0]);
                return true;
            }
        }

        /// <summary>
        /// Read an object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="contentType">The content type of the object.</param>
        /// <param name="data">The object's data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool ReadObject(string key, out string contentType, out byte[] data, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            data = null;
            contentType = null;
            key = key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                string query = ContainerQueries.ReadObject(key);
                DataTable existsResult = _Database.Query(query);
                if (existsResult == null || existsResult.Rows.Count < 1)
                {
                    error = ErrorCode.NotFound;
                    return false;
                }
                else
                {
                    ObjectMetadata md = new ObjectMetadata(existsResult.Rows[0]);
                    contentType = md.ContentType;
                }

                bool success = false;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.Read(Settings.ObjectsDirectory + key, out data, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                if (success) SetLastAccessUtc(key, TimestampUtc(DateTime.Now));
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

        /// <summary>
        /// Read an object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="contentType">The content type of the object.</param>
        /// <param name="contentType">The content length of the object.</param>
        /// <param name="stream">The stream containing the object's data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool ReadObject(string key, out string contentType, out long contentLength, out Stream stream, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            stream = null;
            contentType = null;
            contentLength = 0;

            key = key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                string query = ContainerQueries.ReadObject(key);
                DataTable existsResult = _Database.Query(query);
                if (existsResult == null || existsResult.Rows.Count < 1)
                {
                    error = ErrorCode.NotFound;
                    return false;
                }
                else
                {
                    ObjectMetadata md = new ObjectMetadata(existsResult.Rows[0]);
                    contentType = md.ContentType;
                }

                bool success = false;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.Read(Settings.ObjectsDirectory + key, out contentLength, out stream, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                if (success) SetLastAccessUtc(key, TimestampUtc(DateTime.Now));
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

        /// <summary>
        /// Read a range of bytes from an existing object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="startRange">The position from which data should be read.</param>
        /// <param name="numBytes">The number of bytes to read.</param>
        /// <param name="contentType">The content type of the object.</param>
        /// <param name="data">The object's data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool ReadRangeObject(string key, long startRange, long numBytes, out string contentType, out byte[] data, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            data = null;
            contentType = null;
            ObjectMetadata md = null;

            key = key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                string query = ContainerQueries.ReadObject(key);
                DataTable existsResult = _Database.Query(query);
                if (existsResult == null || existsResult.Rows.Count < 1)
                {
                    error = ErrorCode.NotFound;
                    return false;
                }
                else
                {
                    md = new ObjectMetadata(existsResult.Rows[0]);
                    contentType = md.ContentType;
                }

                if (startRange >= md.ContentLength)
                {
                    error = ErrorCode.OutOfRange;
                    return false;
                }

                if (startRange + numBytes > md.ContentLength)
                {
                    numBytes = Convert.ToInt64(md.ContentLength) - startRange;
                }

                bool success = false;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.ReadRange(Settings.ObjectsDirectory + key, startRange, (int)numBytes, out data, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                if (success) SetLastAccessUtc(key, TimestampUtc(DateTime.Now));
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

        /// <summary>
        /// Read a range of bytes from an existing object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="startRange">The position from which data should be read.</param>
        /// <param name="numBytes">The number of bytes to read.</param>
        /// <param name="contentType">The content type of the object.</param>
        /// <param name="stream">The stream containing the object's data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool ReadRangeObject(string key, long startRange, long numBytes, out string contentType, out Stream stream, out ErrorCode error)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            stream = null;
            contentType = null;
            ObjectMetadata md = null;

            key = key.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(key))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(key);
            }

            try
            {
                string query = ContainerQueries.ReadObject(key);
                DataTable existsResult = _Database.Query(query);
                if (existsResult == null || existsResult.Rows.Count < 1)
                {
                    error = ErrorCode.NotFound;
                    return false;
                }
                else
                {
                    md = new ObjectMetadata(existsResult.Rows[0]);
                    contentType = md.ContentType;
                }

                if (startRange >= md.ContentLength)
                {
                    error = ErrorCode.OutOfRange;
                    return false;
                }

                if (startRange + numBytes > md.ContentLength)
                {
                    numBytes = Convert.ToInt64(md.ContentLength) - startRange;
                }

                bool success = false;

                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.ReadRange(Settings.ObjectsDirectory + key, startRange, (int)numBytes, out stream, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                if (success) SetLastAccessUtc(key, TimestampUtc(DateTime.Now));
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

        /// <summary>
        /// Check if an object exists.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <returns>True if exists.</returns>
        public bool Exists(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = key.ToLower();

            bool metadataExists = false;
            bool objectExists = false;

            string query = ContainerQueries.ReadObject(key);
            DataTable existsResult = _Database.Query(query);
            if (existsResult != null && existsResult.Rows.Count > 0) metadataExists = true;

            switch (Settings.HandlerType)
            {
                case ObjectHandlerType.Disk:
                    objectExists = _DiskHandler.Exists(Settings.ObjectsDirectory + key);
                    break;
                default:
                    throw new Exception("Unknown disk handler type in container settings"); 
            }

            return metadataExists && objectExists;
        }

        /// <summary>
        /// Rename an object.
        /// </summary>
        /// <param name="originalKey">The object's key.</param>
        /// <param name="updatedKey">The key to which the object should be renamed.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public bool RenameObject(string originalKey, string updatedKey, out ErrorCode error)
        { 
            if (String.IsNullOrEmpty(originalKey)) throw new ArgumentNullException(nameof(originalKey));
            if (String.IsNullOrEmpty(updatedKey)) throw new ArgumentNullException(nameof(updatedKey));

            originalKey = originalKey.ToLower();
            updatedKey = updatedKey.ToLower();

            lock (_LockKey)
            {
                if (_LockedKeys.Contains(originalKey))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(originalKey);

                if (_LockedKeys.Contains(updatedKey))
                {
                    error = ErrorCode.Locked;
                    return false;
                }

                _LockedKeys.Add(updatedKey);
            }

            try
            {
                if (!Exists(originalKey))
                {
                    error = ErrorCode.NotFound;
                    return false;
                }

                if (Exists(updatedKey))
                {
                    error = ErrorCode.AlreadyExists;
                    return false;
                }

                bool success = false;
                switch (Settings.HandlerType)
                {
                    case ObjectHandlerType.Disk:
                        success = _DiskHandler.Rename(Settings.ObjectsDirectory + originalKey, Settings.ObjectsDirectory + updatedKey, out error);
                        break;
                    default:
                        throw new Exception("Unknown disk handler type in container settings");
                }

                if (!success) return false;

                string ts = TimestampUtc(DateTime.Now);
                string query = ContainerQueries.RenameObject(originalKey, updatedKey, ts);
                DataTable updateResult = _Database.Query(query);
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
        
        /// <summary>
        /// Return the number of objects within the container.
        /// </summary>
        /// <returns>The number of objects in the container.</returns>
        public long ObjectCount()
        { 
            string query = ContainerQueries.ObjectCount();
            DataTable result = _Database.Query(query);
            if (result != null && result.Rows.Count == 1)
            {
                if (result.Rows[0]["NumObjects"] != null && result.Rows[0]["NumObjects"] != DBNull.Value)
                    return Convert.ToInt64(result.Rows[0]["NumObjects"]);
            }
            return 0;
        }

        /// <summary>
        /// Return the number of bytes consumed by objects within the container.
        /// </summary>
        /// <returns>Number of bytes.</returns>
        public long BytesConsumed()
        {
            string query = ContainerQueries.BytesConsumed();
            DataTable result = _Database.Query(query);
            if (result != null && result.Rows.Count == 1)
            {
                if (result.Rows[0]["Bytes"] != null && result.Rows[0]["Bytes"] != DBNull.Value)
                    return Convert.ToInt64(result.Rows[0]["Bytes"]);
            }
            return 0;
        }

        /// <summary>
        /// Retrieve statistics from the container.
        /// </summary> 
        /// <returns>ContainerMetadata.</returns>
        public ContainerMetadata GetStatistics()
        { 
            ContainerMetadata meta = new ContainerMetadata();
            meta.User = Settings.User;
            meta.Name = Settings.Name;

            meta.IndexStart = null;
            meta.MaxResults = null;
            meta.Filter = null;
            meta.Objects = null;

            meta.TotalCount = ObjectCount();
            meta.TotalBytes = BytesConsumed();

            meta.PublicRead = IsPublicRead();
            meta.PublicWrite = IsPublicWrite();
            meta.LatestEntry = LatestEntry(); 
            return meta;
        }

        /// <summary>
        /// List the metadata of objects in the container.
        /// </summary>
        /// <param name="indexStart">The row index from which to start returning metadata.</param>
        /// <param name="maxResults">The maximum number of results to return.</param>
        /// <returns>ContainerMetadata.</returns>
        public ContainerMetadata Enumerate(int? indexStart, int? maxResults, EnumerationFilter filter, string orderByClause)
        {
            string query = ContainerQueries.Enumerate(indexStart, maxResults, filter, orderByClause);
            DataTable result = _Database.Query(query);
            List<ObjectMetadata> objects = ObjectMetadata.FromDataTable(result);

            ContainerMetadata meta = new ContainerMetadata();
            meta.User = Settings.User;
            meta.Name = Settings.Name;

            meta.IndexStart = indexStart;
            meta.MaxResults = maxResults;
            meta.Filter = filter;

            meta.TotalCount = ObjectCount();
            if (objects != null && objects.Count > 0) meta.Count = objects.Count;

            meta.TotalBytes = BytesConsumed();
            if (objects != null) meta.Bytes = objects.Sum(m => Convert.ToInt64(m.ContentLength));

            meta.PublicRead = IsPublicRead();
            meta.PublicWrite = IsPublicWrite();
            meta.LatestEntry = LatestEntry();
            meta.Objects = objects;
            return meta;
        }

        /// <summary>
        /// Return the timestamp of the latest object entry in the container.
        /// </summary>
        /// <returns>Null or DateTime.</returns>
        public DateTime? LatestEntry()
        {
            string query = ContainerQueries.GetLatestEntry();
            DataTable result = _Database.Query(query);
            if (result == null || result.Rows.Count != 1) return null;

            DataColumnCollection columns = result.Columns;
            if (columns.Contains("LastUpdateUtc"))
            {
                foreach (DataRow currRow in result.Rows)
                {
                    if (currRow["LastUpdateUtc"] != null && currRow["LastUpdateUtc"] != DBNull.Value)
                    {
                        return Convert.ToDateTime(currRow["LastUpdateUtc"]);
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Destroy the container and all of its data.
        /// </summary>
        public void Destroy()
        {
            // close database client
            _Database.Dispose();
            _Database = null;

            // delete files and directories
            File.Delete(Settings.DatabaseFilename);
            Directory.Delete(Settings.ObjectsDirectory, true);
            Directory.Delete(Settings.RootDirectory, true);

            Dispose(true);
        }

        /// <summary>
        /// Add an audit log entry, if audit logging is enabled.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="action">The action performed by the user.</param>
        /// <param name="metadata">Metadata relevant to the operation.</param>
        /// <param name="force">Force writing to the audit log, even if disabled.</param>
        public void AddAuditLogEntry(string key, AuditLogEntryType action, string metadata, bool force)
        {
            if (!force && !IsAuditLogging()) return;

            string ts = TimestampUtc(DateTime.Now);
            string query = ContainerQueries.AddAuditEntry(key, action, metadata, ts);
            _Database.Query(query);
        }

        /// <summary>
        /// Retrieve audit log entries.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="action">The action performed against the object.</param>
        /// <param name="maxResults">Maximum number of results to return.</param>
        /// <param name="index">The starting index of the first result to return.</param>
        /// <param name="createdBefore">Timestamp before which entries must have been created.</param>
        /// <param name="createdAfter">Timestamp after which entries must have been created.</param>
        /// <returns>List of AuditLogEntry.</returns>
        public List<AuditLogEntry> GetAuditLogEntries(string key, string action, int? maxResults, int? index, DateTime? createdBefore, DateTime? createdAfter)
        {
            if (!String.IsNullOrEmpty(key)) key = key.ToLower();

            string query = ContainerQueries.GetAuditEntries(key, action, maxResults, index, createdBefore, createdAfter);
            DataTable result = _Database.Query(query);
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

        /// <summary>
        /// Clear the audit log.
        /// </summary>
        public void ClearAuditLog()
        {
            string query = ContainerQueries.ClearAuditLog();
            DataTable result = _Database.Query(query);
            return;
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
                if (_Database != null) _Database.Dispose();
            }

            _Disposed = true;
        }

        private void WritePropertiesFile()
        {
            lock (_LockPropertiesFile)
            {
                File.WriteAllBytes(_PropertiesFile, Encoding.UTF8.GetBytes(Common.SerializeJson(Settings, true)));
            }
        }

        private void CreateEmptyFile(string filename)
        {
            File.Create(filename).Dispose();
        }

        private void DeleteFileIfExists(string filename)
        {
            if (File.Exists(filename)) File.Delete(filename);
        }

        private string TimestampUtc()
        {
            return DateTime.Now.ToUniversalTime().ToString(_TimestampFormat);
        }

        private string TimestampUtc(DateTime ts)
        {
            return ts.ToUniversalTime().ToString(_TimestampFormat);
        }

        private void InitializeContainerDatabase()
        {
            string createObjTableQuery = ContainerQueries.CreateObjectsTableQuery();
            _Database.Query(createObjTableQuery);

            string createAuditTableQuery = ContainerQueries.CreateAuditLogTableQuery();
            _Database.Query(createAuditTableQuery);
        }

        private void SetLastAccessUtc(string key, string ts)
        {
            string query = ContainerQueries.SetLastAccess(key, ts);
            _Database.Query(query);
        }

        private void SetLastpdateUtc(string key, string ts)
        {
            string query = ContainerQueries.SetLastUpdate(key, ts);
            _Database.Query(query);
        }

        private void SetObjectSize(string key, long size)
        {
            string query = ContainerQueries.SetObjectSize(key, size);
            _Database.Query(query);
        }
        
        private void SetMd5(string key, string md5)
        {
            string query = ContainerQueries.SetMd5(key, md5);
            DataTable result = _Database.Query(query);
            return;
        }

        private string GetMd5(string key)
        {
            string query = ContainerQueries.GetMd5(key);
            DataTable result = _Database.Query(query);
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

        #endregion
    }
}
