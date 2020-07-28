using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using Watson.ORM;
using Watson.ORM.Core;
using Kvpbase.StorageServer.Classes.Managers; 
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer.Classes.Handlers
{
    internal class ObjectHandler
    { 
        private Settings _Settings;
        private LoggingModule _Logging;
        private WatsonORM _ORM;
        private LockManager _Locks;
        private static string _Header = "[Kvpbase.ObjectHandler] ";

        internal ObjectHandler(Settings settings, LoggingModule logging, WatsonORM orm, LockManager locks)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (orm == null) throw new ArgumentNullException(nameof(orm));
            if (locks == null) throw new ArgumentNullException(nameof(locks));

            _Settings = settings;
            _Logging = logging;
            _ORM = orm;
            _Locks = locks;
        }

        internal bool Delete(
            RequestMetadata md,
            ContainerClient client, 
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "Delete " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddWriteLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }
             
            if (!client.RemoveObject(md.Params.ObjectKey, out error))
            {
                _Logging.Warn(header + "unable to remove object: " + error.ToString());
                _Locks.RemoveWriteLock(lockGuid);
                return false;
            }
            else
            {
                string logData =
                    "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                    "User: " + md.Params.UserGUID;
                 
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.Delete, logData, false);
                _Locks.RemoveWriteLock(lockGuid);
                _Logging.Debug(header + "deleted object");
                return true;
            } 
        }

        internal bool Read(
            RequestMetadata md,
            ContainerClient client,  
            out string contentType,
            out long contentLength,
            out Stream stream,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            contentType = null;
            contentLength = 0;
            stream = null;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "Read " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddReadLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add read lock");
                return false;
            }

            if (md.Params.Index != null && md.Params.Count != null)
            {
                #region Range-Read

                if (!client.ReadRangeObject(md.Params.ObjectKey, Convert.ToInt64(md.Params.Index), Convert.ToInt32(md.Params.Count), out contentType, out stream, out error))
                {
                    _Logging.Warn(header + "unable to read range: " + error.ToString());
                    _Locks.RemoveReadLock(lockGuid);
                    return false;
                }
                else
                {
                    _Locks.RemoveReadLock(lockGuid);
                    string logData =
                        "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                        "User: " + md.Params.UserGUID;
                    logData += " Index: " + md.Params.Index;
                    logData += " Count: " + md.Params.Count;
                    client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.ReadRange, logData, false);
                    contentLength = (long)md.Params.Count;
                    return true;
                }

                #endregion
            }
            else
            {
                #region Full-Read

                if (!client.ReadObject(md.Params.ObjectKey, out contentType, out contentLength, out stream, out error))
                {
                    _Logging.Warn(header + "unable to read object: " + error.ToString());
                    _Locks.RemoveReadLock(lockGuid);
                    return false;
                }
                else
                {
                    _Locks.RemoveReadLock(lockGuid);
                    string logData =
                        "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                        "User: " + md.Params.UserGUID;
                    client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.Read, logData, false);
                    return true;
                }

                #endregion
            } 
        }

        internal bool Exists(
            RequestMetadata md, 
            ContainerClient client)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "Exists " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            if (!client.Exists(md.Params.ObjectKey))
            {
                _Logging.Debug(header + "object does not exist");
                return false;
            }
            else
            { 
                string logData =
                   "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                   "User: " + md.Params.UserGUID;
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.Exists, logData, false);
                return true;
            } 
        }

        internal bool Create(
            RequestMetadata md,
            ContainerClient client,
            byte[] data,  
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "Create " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddWriteLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObject(md.Params.ObjectKey, md.Http.Request.ContentType, md.Http.Request.ContentLength, md.Http.Request.Data, md.Params.Tags, out error))
            {
                _Logging.Warn(header + "unable to write object: " + error.ToString());
                _Locks.RemoveWriteLock(lockGuid);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(lockGuid);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                    "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                    "User: " + md.Params.UserGUID + " " +
                    "Bytes: " + dataLen;
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.Write, logData, false);
                _Logging.Debug(header + "created object");
                return true;
            } 
        }

        internal bool Create(
            RequestMetadata md,
            ContainerClient client,
            long contentLength,
            Stream stream,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "Create " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddWriteLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObject(md.Params.ObjectKey, md.Http.Request.ContentType, contentLength, stream, md.Params.Tags, out error))
            {
                _Logging.Warn(header + "unable to write object: " + error.ToString());
                _Locks.RemoveWriteLock(lockGuid);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(lockGuid);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                   "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                   "User: " + md.Params.UserGUID + " " +
                   "Bytes: " + dataLen;
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.Write, logData, false);
                _Logging.Debug(header + "created object");
                return true;
            }
        }

        internal bool Rename(
            RequestMetadata md,
            ContainerClient client, 
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "Rename " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddWriteLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.RenameObject(md.Params.ObjectKey, md.Params.Rename, out error))
            {
                _Logging.Warn(header + "unable to rename object: " + error.ToString());
                _Locks.RemoveWriteLock(lockGuid);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(lockGuid);
                string logData =
                   "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                   "User: " + md.Params.UserGUID + " " +
                   "RenameTo: " + md.Params.Rename;
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.Rename, logData, true);
                _Logging.Debug(header + "renamed object");
                return true;
            } 
        }

        internal bool WriteRange(
            RequestMetadata md,
            ContainerClient client,  
            byte[] data, 
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "WriteRange " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddWriteLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteRangeObject(md.Params.ObjectKey, Convert.ToInt64(md.Params.Index), data, out error))
            {
                _Logging.Warn(header + "unable to write range: " + error.ToString());
                _Locks.RemoveWriteLock(lockGuid);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(lockGuid);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGUID + " " +
                  "Index: " + Convert.ToInt64(md.Params.Index) + " " +
                  "Bytes: " + dataLen;
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.WriteRange, logData, true);
                _Logging.Debug(header + "wrote object range");
                return true;
            } 
        }

        internal bool WriteRange(
            RequestMetadata md,
            ContainerClient client,
            long numBytes,
            Stream stream,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "WriteRange " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddWriteLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteRangeObject(md.Params.ObjectKey, Convert.ToInt64(md.Params.Index), numBytes, stream, out error))
            {
                _Logging.Warn(header + "unable to write range: " + error.ToString());
                _Locks.RemoveWriteLock(lockGuid);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(lockGuid);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGUID + " " +
                  "Index: " + Convert.ToInt64(md.Params.Index) + " " +
                  "Bytes: " + dataLen;
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.WriteRange, logData, true);
                _Logging.Debug(header + "wrote object range");
                return true;
            }
        }

        internal bool WriteTags(
            RequestMetadata md,
            ContainerClient client,  
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "WriteTags " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddWriteLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectTags(md.Params.ObjectKey, md.Params.Tags, out error))
            {
                _Logging.Warn(header + "unable to write tags: " + error.ToString());
                _Locks.RemoveWriteLock(lockGuid);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(lockGuid); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGUID + " " +
                  "Tags: " + md.Params.Tags;
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.WriteTags, logData, true);
                _Logging.Debug(header + "wrote object tags");
                return true;
            }
        }

        internal bool DeleteTags(
            RequestMetadata md,
            ContainerClient client, 
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "DeleteTags " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddWriteLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectTags(md.Params.ObjectKey, null, out error))
            {
                _Logging.Warn(header + "unable to delete tags: " + error.ToString());
                _Locks.RemoveWriteLock(lockGuid);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(lockGuid); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGUID;
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.DeleteTags, logData, true);
                _Logging.Debug(header + "deleted object tags");
                return true;
            }
        }

        internal bool WriteKeyValues(
            RequestMetadata md,
            ContainerClient client, 
            Dictionary<string, string> dict,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "WriteKeyValues " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddWriteLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectKeyValuePairs(md.Params.ObjectKey, dict, out error))
            {
                _Logging.Warn(header + "unable to write key values: " + error.ToString());
                _Locks.RemoveWriteLock(lockGuid);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(lockGuid); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGUID;
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.WriteKeyValue, logData, true);
                _Logging.Debug(header + "wrote key values");
                return true;
            }
        }

        internal bool DeleteKeyValues(
            RequestMetadata md,
            ContainerClient client, 
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "DeleteKeyValues " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            string lockGuid = _Locks.AddWriteLock(md);
            if (String.IsNullOrEmpty(lockGuid))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectKeyValuePairs(md.Params.ObjectKey, null, out error))
            {
                _Logging.Warn(header + "unable to delete key values: " + error.ToString());
                _Locks.RemoveWriteLock(lockGuid);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(lockGuid); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGUID;
                client.AddAuditLogEntry(md.Params.ObjectKey, AuditLogEntryType.DeleteKeyValue, logData, true);
                _Logging.Debug(header + "deleted key values");
                return true;
            }
        }

        internal bool ReadKeyValues(
            RequestMetadata md,
            ContainerClient client, 
            out Dictionary<string, string> dict,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            dict = new Dictionary<string, string>();
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "ReadKeyValues " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            if (!client.ReadObjectKeyValues(md.Params.ObjectKey, out dict, out error))
            {
                _Logging.Warn(header + "unable to read key values: " + error.ToString());
                return false;
            }

            return true; 
        } 

        internal bool Lock(
            RequestMetadata md,
            ContainerClient client, 
            out string lockGuid,
            out ErrorCode error)
        {
            lockGuid = null;
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "Lock " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            if (md.Params.ExpirationUtc != null)
            {
                if (md.Params.WriteLock)
                {
                    if (_Locks.LockExists(md.Http.Request.RawUrlWithoutQuery))
                    {
                        _Logging.Warn(header + "lock already exists");
                        error = ErrorCode.AlreadyExists;
                        return false;
                    }

                    lockGuid = _Locks.AddWriteLock(md); 
                }
                else if (md.Params.ReadLock)
                {
                    if (_Locks.LockExists(md.Http.Request.RawUrlWithoutQuery))
                    {
                        _Logging.Warn(header + "already exists");
                        error = ErrorCode.AlreadyExists;
                        return false;
                    }

                    lockGuid = _Locks.AddReadLock(md);
                }
            }
            else
            {
                _Logging.Warn(header + "no expiration supplied");
                error = ErrorCode.OutOfRange;
                return false;
            }

            if (!String.IsNullOrEmpty(lockGuid))
            {
                return true;
            }
            else
            {
                error = ErrorCode.AlreadyExists;
                return false;
            }
        }

        internal bool Unlock(
            RequestMetadata md,
            ContainerClient client, 
            out ErrorCode errorCode)
        {
            errorCode = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client)); 
            string header = _Header + "Unlock " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            if (!String.IsNullOrEmpty(md.Params.LockGUID))
            {
                if (md.Params.WriteLock)
                {
                    _Locks.RemoveWriteLock(md.Params.LockGUID);
                    return true;
                }
                else if (md.Params.ReadLock)
                {
                    _Locks.RemoveReadLock(md.Params.LockGUID);
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                errorCode = ErrorCode.NotFound;
                return false;
            }
        }

        internal bool GetLocks(
            RequestMetadata md,
            ContainerClient client,
            out List<UrlLock> locks,
            out ErrorCode errorCode)
        {
            errorCode = ErrorCode.None;
            locks = new List<UrlLock>();
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            string header = _Header + "Unlock " + client.Container.UserGUID + "/" + client.Container.Name + "/" + md.Params.ObjectKey + " ";

            if (md.Params.WriteLock)
            {
                locks = _Locks.GetWriteLocks();
                return true;
            }
            else if (md.Params.ReadLock)
            {
                locks = _Locks.GetReadLocks();
                return true;
            }
            else
            {
                errorCode = ErrorCode.NotFound;
                return false;
            }
        }
    }
}
