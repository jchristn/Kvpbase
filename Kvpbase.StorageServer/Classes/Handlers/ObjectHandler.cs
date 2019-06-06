using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;

using Kvpbase.Classes.Managers;
using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase.Classes.Handlers
{
    public class ObjectHandler
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private UrlLockManager _UrlLockMgr;

        #endregion

        #region Constructors-and-Factories

        public ObjectHandler(Settings settings, LoggingModule logging, UrlLockManager urlLock)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (urlLock == null) throw new ArgumentNullException(nameof(urlLock));
            
            _Settings = settings;
            _Logging = logging;
            _UrlLockMgr = urlLock;
        }

        #endregion

        #region Public-Methods

        public bool Delete(
            RequestMetadata md,
            Container container,
            string objectName,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            if (!_UrlLockMgr.AddWriteLock(md))
            {
                return false;
            }
              
            if (!container.RemoveObject(objectName, out error))
            {
                return false;
            }
            else
            {
                string logData =
                   "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                   "User: " + md.Params.UserGuid;

                container.AddAuditLogEntry(objectName, AuditLogEntryType.Delete, logData, false);
                return true;
            } 
        }

        public bool Read(
            RequestMetadata md,
            Container container,
            string objectName,
            long? indexStart,
            int? count,
            out string contentType,
            out byte[] data,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            contentType = null;
            data = null;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            if (!_UrlLockMgr.AddReadLock(md))
            {
                return false;
            }
              
            if (indexStart != null && count != null)
            {
                #region Range-Read

                if (!container.ReadRangeObject(objectName, Convert.ToInt64(indexStart), Convert.ToInt32(count), out contentType, out data, out error))
                {
                    _UrlLockMgr.RemoveReadLock(md);
                    return false;
                }
                else
                {
                    _UrlLockMgr.RemoveReadLock(md);
                    string logData =
                       "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                       "User: " + md.Params.UserGuid;
                    logData += " Index: " + indexStart;
                    logData += " Count: " + count;
                    container.AddAuditLogEntry(objectName, AuditLogEntryType.ReadRange, logData, false);
                    return true;
                }

                #endregion
            }
            else
            {
                #region Full-Read

                if (!container.ReadObject(objectName, out contentType, out data, out error))
                {
                    _UrlLockMgr.RemoveReadLock(md);
                    return false;
                }
                else
                {
                    _UrlLockMgr.RemoveReadLock(md);
                    string logData =
                       "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                       "User: " + md.Params.UserGuid;
                    container.AddAuditLogEntry(objectName, AuditLogEntryType.Read, logData, false);
                    return true;
                }

                #endregion
            } 
        }

        public bool Read(
            RequestMetadata md,
            Container container,
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
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            if (!_UrlLockMgr.AddReadLock(md))
            {
                return false;
            }

            if (indexStart != null && count != null)
            {
                #region Range-Read

                if (!container.ReadRangeObject(objectName, Convert.ToInt64(indexStart), Convert.ToInt32(count), out contentType, out stream, out error))
                {
                    _UrlLockMgr.RemoveReadLock(md);
                    return false;
                }
                else
                {
                    _UrlLockMgr.RemoveReadLock(md);
                    string logData =
                       "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                       "User: " + md.Params.UserGuid;
                    logData += " Index: " + indexStart;
                    logData += " Count: " + count;
                    container.AddAuditLogEntry(objectName, AuditLogEntryType.ReadRange, logData, false);
                    return true;
                }

                #endregion
            }
            else
            {
                #region Full-Read

                if (!container.ReadObject(objectName, out contentType, out contentLength, out stream, out error))
                {
                    _UrlLockMgr.RemoveReadLock(md);
                    return false;
                }
                else
                {
                    _UrlLockMgr.RemoveReadLock(md);
                    string logData =
                       "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                       "User: " + md.Params.UserGuid;
                    container.AddAuditLogEntry(objectName, AuditLogEntryType.Read, logData, false);
                    return true;
                }

                #endregion
            }
        }

        public bool Exists(
            RequestMetadata md, 
            Container container,
            string objectName)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
             
            if (!container.Exists(objectName))
            { 
                return false;
            }
            else
            { 
                string logData =
                   "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                   "User: " + md.Params.UserGuid;
                container.AddAuditLogEntry(objectName, AuditLogEntryType.Exists, logData, false);
                return true;
            } 
        }

        public bool Create(
            RequestMetadata md,
            Container container,
            string objectName,
            string contentType,
            byte[] data,  
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            if (!_UrlLockMgr.AddWriteLock(md))
            {
                return false;
            }

            if (!container.WriteObject(objectName, md.Http.ContentType, md.Http.Data, Common.CsvToStringList(md.Params.Tags), out error))
            {
                _UrlLockMgr.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _UrlLockMgr.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Data != null) dataLen = md.Http.Data.Length;
                string logData =
                   "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                   "User: " + md.Params.UserGuid + " " +
                   "Bytes: " + dataLen;
                container.AddAuditLogEntry(objectName, AuditLogEntryType.Write, logData, false);
                return true;
            } 
        }

        public bool Create(
            RequestMetadata md,
            Container container,
            string objectName,
            string contentType,
            long contentLength,
            Stream stream,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            if (!_UrlLockMgr.AddWriteLock(md))
            {
                return false;
            }

            if (!container.WriteObject(objectName, contentType, contentLength, stream, Common.CsvToStringList(md.Params.Tags), out error))
            {
                _UrlLockMgr.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _UrlLockMgr.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Data != null) dataLen = md.Http.Data.Length;
                string logData =
                   "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                   "User: " + md.Params.UserGuid + " " +
                   "Bytes: " + dataLen;
                container.AddAuditLogEntry(objectName, AuditLogEntryType.Write, logData, false);
                return true;
            }
        }

        public bool Rename(
            RequestMetadata md,
            Container container,
            string originalName,
            string updatedName,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(originalName)) throw new ArgumentNullException(nameof(originalName));
            if (String.IsNullOrEmpty(updatedName)) throw new ArgumentNullException(nameof(updatedName));
             
            if (!_UrlLockMgr.AddWriteLock(md))
            {
                return false;
            }

            if (!container.RenameObject(originalName, updatedName, out error))
            {
                _UrlLockMgr.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _UrlLockMgr.RemoveWriteLock(md);
                string logData =
                   "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                   "User: " + md.Params.UserGuid + " " +
                   "RenameTo: " + updatedName;
                container.AddAuditLogEntry(originalName, AuditLogEntryType.Rename, logData, true);
                return true;
            } 
        }

        public bool WriteRange(
            RequestMetadata md,
            Container container,
            string objectName,
            long indexStart,
            byte[] data, 
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            if (!_UrlLockMgr.AddWriteLock(md))
            {
                return false;
            }

            if (!container.WriteRangeObject(objectName, indexStart, data, out error))
            {
                _UrlLockMgr.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _UrlLockMgr.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Data != null) dataLen = md.Http.Data.Length;
                string logData =
                  "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                  "User: " + md.Params.UserGuid + " " +
                  "Index: " + indexStart + " " +
                  "Bytes: " + dataLen;
                container.AddAuditLogEntry(objectName, AuditLogEntryType.WriteRange, logData, true);
                return true;
            } 
        }

        public bool WriteRange(
            RequestMetadata md,
            Container container,
            string objectName,
            long indexStart,
            long numBytes,
            Stream stream,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            if (!_UrlLockMgr.AddWriteLock(md))
            {
                return false;
            }

            if (!container.WriteRangeObject(objectName, indexStart, numBytes, stream, out error))
            {
                _UrlLockMgr.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _UrlLockMgr.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Data != null) dataLen = md.Http.Data.Length;
                string logData =
                  "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                  "User: " + md.Params.UserGuid + " " +
                  "Index: " + indexStart + " " +
                  "Bytes: " + dataLen;
                container.AddAuditLogEntry(objectName, AuditLogEntryType.WriteRange, logData, true);
                return true;
            }
        }

        public bool WriteTags(
            RequestMetadata md,
            Container container,
            string objectName,
            string tags,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (String.IsNullOrEmpty(tags)) throw new ArgumentNullException(nameof(tags));

            if (!_UrlLockMgr.AddWriteLock(md))
            {
                return false;
            }

            if (!container.WriteObjectTags(objectName, tags, out error))
            {
                _UrlLockMgr.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _UrlLockMgr.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Data != null) dataLen = md.Http.Data.Length;
                string logData =
                  "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                  "User: " + md.Params.UserGuid + " " +
                  "Tags: " + tags;
                container.AddAuditLogEntry(objectName, AuditLogEntryType.WriteTags, logData, true);
                return true;
            }
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
