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
using Kvpbase.StorageServer.Classes;

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
            string objectName,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "Delete " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Locks.AddWriteLock(md))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }
             
            if (!client.RemoveObject(objectName, out error))
            {
                _Logging.Warn(header + "unable to remove object: " + error.ToString());
                _Locks.RemoveWriteLock(md);
                return false;
            }
            else
            {
                string logData =
                    "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                    "User: " + md.Params.UserGuid;
                 
                client.AddAuditLogEntry(objectName, AuditLogEntryType.Delete, logData, false);
                _Locks.RemoveWriteLock(md);
                _Logging.Debug(header + "deleted object");
                return true;
            } 
        }

        internal bool Read(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            long? indexStart,
            int? count,
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
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "Read " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Locks.AddReadLock(md))
            {
                _Logging.Warn(header + "unable to add read lock");
                return false;
            }

            if (indexStart != null && count != null)
            {
                #region Range-Read

                if (!client.ReadRangeObject(objectName, Convert.ToInt64(indexStart), Convert.ToInt32(count), out contentType, out stream, out error))
                {
                    _Logging.Warn(header + "unable to read range: " + error.ToString());
                    _Locks.RemoveReadLock(md);
                    return false;
                }
                else
                {
                    _Locks.RemoveReadLock(md);
                    string logData =
                        "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                        "User: " + md.Params.UserGuid;
                    logData += " Index: " + indexStart;
                    logData += " Count: " + count;
                    client.AddAuditLogEntry(objectName, AuditLogEntryType.ReadRange, logData, false);
                    contentLength = (long)count;
                    return true;
                }

                #endregion
            }
            else
            {
                #region Full-Read

                if (!client.ReadObject(objectName, out contentType, out contentLength, out stream, out error))
                {
                    _Logging.Warn(header + "unable to read object: " + error.ToString());
                    _Locks.RemoveReadLock(md);
                    return false;
                }
                else
                {
                    _Locks.RemoveReadLock(md);
                    string logData =
                        "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                        "User: " + md.Params.UserGuid;
                    client.AddAuditLogEntry(objectName, AuditLogEntryType.Read, logData, false);
                    return true;
                }

                #endregion
            } 
        }

        internal bool Exists(
            RequestMetadata md, 
            ContainerClient client,
            string objectName)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "Exists " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!client.Exists(objectName))
            {
                _Logging.Debug(header + "object does not exist");
                return false;
            }
            else
            { 
                string logData =
                   "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                   "User: " + md.Params.UserGuid;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.Exists, logData, false);
                return true;
            } 
        }

        internal bool Create(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            string contentType,
            byte[] data,  
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "Create " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Locks.AddWriteLock(md))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObject(objectName, md.Http.Request.ContentType, md.Http.Request.ContentLength, md.Http.Request.Data, md.Params.Tags, out error))
            {
                _Logging.Warn(header + "unable to write object: " + error.ToString());
                _Locks.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                    "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                    "User: " + md.Params.UserGuid + " " +
                    "Bytes: " + dataLen;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.Write, logData, false);
                _Logging.Debug(header + "created object");
                return true;
            } 
        }

        internal bool Create(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            string contentType,
            long contentLength,
            Stream stream,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "Create " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Locks.AddWriteLock(md))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObject(objectName, contentType, contentLength, stream, md.Params.Tags, out error))
            {
                _Logging.Warn(header + "unable to write object: " + error.ToString());
                _Locks.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                   "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                   "User: " + md.Params.UserGuid + " " +
                   "Bytes: " + dataLen;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.Write, logData, false);
                _Logging.Debug(header + "created object");
                return true;
            }
        }

        internal bool Rename(
            RequestMetadata md,
            ContainerClient client,
            string originalName,
            string updatedName,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(originalName)) throw new ArgumentNullException(nameof(originalName));
            if (String.IsNullOrEmpty(updatedName)) throw new ArgumentNullException(nameof(updatedName));
            string header = _Header + "Rename " + client.Container.UserGUID + "/" + client.Container.Name + "/" + originalName + " ";

            if (!_Locks.AddWriteLock(md))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.RenameObject(originalName, updatedName, out error))
            {
                _Logging.Warn(header + "unable to rename object: " + error.ToString());
                _Locks.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(md);
                string logData =
                   "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                   "User: " + md.Params.UserGuid + " " +
                   "RenameTo: " + updatedName;
                client.AddAuditLogEntry(originalName, AuditLogEntryType.Rename, logData, true);
                _Logging.Debug(header + "renamed object");
                return true;
            } 
        }

        internal bool WriteRange(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            long indexStart,
            byte[] data, 
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "WriteRange " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Locks.AddWriteLock(md))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteRangeObject(objectName, indexStart, data, out error))
            {
                _Logging.Warn(header + "unable to write range: " + error.ToString());
                _Locks.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid + " " +
                  "Index: " + indexStart + " " +
                  "Bytes: " + dataLen;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.WriteRange, logData, true);
                _Logging.Debug(header + "wrote object range");
                return true;
            } 
        }

        internal bool WriteRange(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            long indexStart,
            long numBytes,
            Stream stream,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "WriteRange " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Locks.AddWriteLock(md))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteRangeObject(objectName, indexStart, numBytes, stream, out error))
            {
                _Logging.Warn(header + "unable to write range: " + error.ToString());
                _Locks.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid + " " +
                  "Index: " + indexStart + " " +
                  "Bytes: " + dataLen;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.WriteRange, logData, true);
                _Logging.Debug(header + "wrote object range");
                return true;
            }
        }

        internal bool WriteTags(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            string tags,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "WriteTags " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Locks.AddWriteLock(md))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectTags(objectName, tags, out error))
            {
                _Logging.Warn(header + "unable to write tags: " + error.ToString());
                _Locks.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(md); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid + " " +
                  "Tags: " + tags;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.WriteTags, logData, true);
                _Logging.Debug(header + "wrote object tags");
                return true;
            }
        }

        internal bool DeleteTags(
            RequestMetadata md,
            ContainerClient client,
            string objectName, 
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "DeleteTags " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Locks.AddWriteLock(md))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectTags(objectName, null, out error))
            {
                _Logging.Warn(header + "unable to delete tags: " + error.ToString());
                _Locks.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(md); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.DeleteTags, logData, true);
                _Logging.Debug(header + "deleted object tags");
                return true;
            }
        }

        internal bool WriteKeyValues(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            Dictionary<string, string> dict,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "WriteKeyValues " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Locks.AddWriteLock(md))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectKeyValuePairs(objectName, dict, out error))
            {
                _Logging.Warn(header + "unable to write key values: " + error.ToString());
                _Locks.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(md); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.WriteKeyValue, logData, true);
                _Logging.Debug(header + "wrote key values");
                return true;
            }
        }

        internal bool DeleteKeyValues(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "DeleteKeyValues " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Locks.AddWriteLock(md))
            {
                _Logging.Warn(header + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectKeyValuePairs(objectName, null, out error))
            {
                _Logging.Warn(header + "unable to delete key values: " + error.ToString());
                _Locks.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Locks.RemoveWriteLock(md); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.DeleteKeyValue, logData, true);
                _Logging.Debug(header + "deleted key values");
                return true;
            }
        }

        internal bool ReadKeyValues(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            out Dictionary<string, string> dict,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            dict = new Dictionary<string, string>();
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string header = _Header + "ReadKeyValues " + client.Container.UserGUID + "/" + client.Container.Name + "/" + objectName + " ";

            if (!client.ReadObjectKeyValues(objectName, out dict, out error))
            {
                _Logging.Warn(header + "unable to read key values: " + error.ToString());
                return false;
            }

            return true; 
        } 
    }
}
