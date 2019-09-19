using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;

using Kvpbase.Classes.Managers;
using Kvpbase.Containers;
using Kvpbase.Classes;

namespace Kvpbase.Classes.Handlers
{
    public class ObjectHandler
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private ConfigManager _Config;

        #endregion

        #region Constructors-and-Factories

        public ObjectHandler(Settings settings, LoggingModule logging, ConfigManager config)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (config == null) throw new ArgumentNullException(nameof(config));

            _Settings = settings;
            _Logging = logging;
            _Config = config;
        }

        #endregion

        #region Public-Methods

        public bool Delete(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " Delete " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Config.AddWriteLock(md))
            {
                _Logging.Warn(logMessage + "unable to add write lock");
                return false;
            }

            if (!client.RemoveObject(objectName, out error))
            {
                _Logging.Warn(logMessage + "unable to remove object: " + error.ToString());
                _Config.RemoveWriteLock(md);
                return false;
            }
            else
            {
                string logData =
                    "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                    "User: " + md.Params.UserGuid;

                client.AddAuditLogEntry(objectName, AuditLogEntryType.Delete, logData, false);
                _Config.RemoveWriteLock(md);
                _Logging.Debug(logMessage + "deleted object");
                return true;
            } 
        }
         
        public bool Read(
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
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " Read " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Config.AddReadLock(md))
            {
                _Logging.Warn(logMessage + "unable to add read lock");
                return false;
            }

            if (indexStart != null && count != null)
            {
                #region Range-Read

                if (!client.ReadRangeObject(objectName, Convert.ToInt64(indexStart), Convert.ToInt32(count), out contentType, out stream, out error))
                {
                    _Logging.Warn(logMessage + "unable to read range: " + error.ToString());
                    _Config.RemoveReadLock(md);
                    return false;
                }
                else
                {
                    _Config.RemoveReadLock(md);
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
                    _Logging.Warn(logMessage + "unable to read object: " + error.ToString());
                    _Config.RemoveReadLock(md);
                    return false;
                }
                else
                {
                    _Config.RemoveReadLock(md);
                    string logData =
                        "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                        "User: " + md.Params.UserGuid;
                    client.AddAuditLogEntry(objectName, AuditLogEntryType.Read, logData, false);
                    return true;
                }

                #endregion
            } 
        }
        
        public bool Exists(
            RequestMetadata md, 
            ContainerClient client,
            string objectName)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " Exists " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!client.Exists(objectName))
            {
                _Logging.Debug(logMessage + "object does not exist");
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

        public bool Create(
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
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " Create " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";
             
            if (!_Config.AddWriteLock(md))
            {
                _Logging.Warn(logMessage + "unable to add write lock");
                return false;
            }

            if (!client.WriteObject(objectName, md.Http.Request.ContentType, md.Http.Request.ContentLength, md.Http.Request.Data, Common.CsvToStringList(md.Params.Tags), out error))
            {
                _Logging.Warn(logMessage + "unable to write object: " + error.ToString());
                _Config.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Config.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                    "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                    "User: " + md.Params.UserGuid + " " +
                    "Bytes: " + dataLen;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.Write, logData, false);
                _Logging.Debug(logMessage + "created object");
                return true;
            } 
        }

        public bool Create(
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
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " Create " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Config.AddWriteLock(md))
            {
                _Logging.Warn(logMessage + "unable to add write lock");
                return false;
            }

            if (!client.WriteObject(objectName, contentType, contentLength, stream, Common.CsvToStringList(md.Params.Tags), out error))
            {
                _Logging.Warn(logMessage + "unable to write object: " + error.ToString());
                _Config.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Config.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                   "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                   "User: " + md.Params.UserGuid + " " +
                   "Bytes: " + dataLen;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.Write, logData, false);
                _Logging.Debug(logMessage + "created object");
                return true;
            }
        }

        public bool Rename(
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
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " Rename " + client.Container.UserGuid + "/" + client.Container.Name + "/" + originalName + " to " + updatedName + " ";

            if (!_Config.AddWriteLock(md))
            {
                _Logging.Warn(logMessage + "unable to add write lock");
                return false;
            }

            if (!client.RenameObject(originalName, updatedName, out error))
            {
                _Logging.Warn(logMessage + "unable to rename object: " + error.ToString());
                _Config.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Config.RemoveWriteLock(md);
                string logData =
                   "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                   "User: " + md.Params.UserGuid + " " +
                   "RenameTo: " + updatedName;
                client.AddAuditLogEntry(originalName, AuditLogEntryType.Rename, logData, true);
                _Logging.Debug(logMessage + "renamed object");
                return true;
            } 
        }

        public bool WriteRange(
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
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " WriteRange " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Config.AddWriteLock(md))
            {
                _Logging.Warn(logMessage + "unable to add write lock");
                return false;
            }

            if (!client.WriteRangeObject(objectName, indexStart, data, out error))
            {
                _Logging.Warn(logMessage + "unable to write range: " + error.ToString());
                _Config.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Config.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid + " " +
                  "Index: " + indexStart + " " +
                  "Bytes: " + dataLen;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.WriteRange, logData, true);
                _Logging.Debug(logMessage + "wrote object range");
                return true;
            } 
        }

        public bool WriteRange(
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
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " WriteRange " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Config.AddWriteLock(md))
            {
                _Logging.Warn(logMessage + "unable to add write lock");
                return false;
            }

            if (!client.WriteRangeObject(objectName, indexStart, numBytes, stream, out error))
            {
                _Logging.Warn(logMessage + "unable to write range: " + error.ToString());
                _Config.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Config.RemoveWriteLock(md);
                int dataLen = 0;
                if (md.Http.Request.Data != null) dataLen = (int)md.Http.Request.ContentLength;
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid + " " +
                  "Index: " + indexStart + " " +
                  "Bytes: " + dataLen;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.WriteRange, logData, true);
                _Logging.Debug(logMessage + "wrote object range");
                return true;
            }
        }

        public bool WriteTags(
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
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " WriteTags " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Config.AddWriteLock(md))
            {
                _Logging.Warn(logMessage + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectTags(objectName, tags, out error))
            {
                _Logging.Warn(logMessage + "unable to write tags: " + error.ToString());
                _Config.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Config.RemoveWriteLock(md); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid + " " +
                  "Tags: " + tags;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.WriteTags, logData, true);
                _Logging.Debug(logMessage + "wrote object tags");
                return true;
            }
        }

        public bool DeleteTags(
            RequestMetadata md,
            ContainerClient client,
            string objectName, 
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " DeleteTags " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Config.AddWriteLock(md))
            {
                _Logging.Warn(logMessage + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectTags(objectName, null, out error))
            {
                _Logging.Warn(logMessage + "unable to delete tags: " + error.ToString());
                _Config.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Config.RemoveWriteLock(md); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.DeleteTags, logData, true);
                _Logging.Debug(logMessage + "deleted object tags");
                return true;
            }
        }

        public bool WriteKeyValues(
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
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " WriteKeyValues " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Config.AddWriteLock(md))
            {
                _Logging.Warn(logMessage + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectKeyValuePairs(objectName, dict, out error))
            {
                _Logging.Warn(logMessage + "unable to write key values: " + error.ToString());
                _Config.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Config.RemoveWriteLock(md); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.WriteKeyValue, logData, true);
                _Logging.Debug(logMessage + "wrote key values");
                return true;
            }
        }

        public bool DeleteKeyValues(
            RequestMetadata md,
            ContainerClient client,
            string objectName,
            out ErrorCode error)
        {
            error = ErrorCode.None;
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " DeleteKeyValues " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!_Config.AddWriteLock(md))
            {
                _Logging.Warn(logMessage + "unable to add write lock");
                return false;
            }

            if (!client.WriteObjectKeyValuePairs(objectName, null, out error))
            {
                _Logging.Warn(logMessage + "unable to delete key values: " + error.ToString());
                _Config.RemoveWriteLock(md);
                return false;
            }
            else
            {
                _Config.RemoveWriteLock(md); 
                string logData =
                  "Source: " + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " " +
                  "User: " + md.Params.UserGuid;
                client.AddAuditLogEntry(objectName, AuditLogEntryType.DeleteKeyValue, logData, true);
                _Logging.Debug(logMessage + "deleted key values");
                return true;
            }
        }

        public bool ReadKeyValues(
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
            string logMessage = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ReadKeyValues " + client.Container.UserGuid + "/" + client.Container.Name + "/" + objectName + " ";

            if (!client.ReadObjectKeyValues(objectName, out dict, out error))
            {
                _Logging.Warn(logMessage + "unable to read key values: " + error.ToString());
                return false;
            }

            return true; 
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
