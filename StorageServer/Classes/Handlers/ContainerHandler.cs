using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;

namespace Kvpbase
{
    public class ContainerHandler
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private ContainerManager _ContainerMgr;

        #endregion

        #region Constructors-and-Factories

        public ContainerHandler(Settings settings, LoggingModule logging, ContainerManager containers)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (containers == null) throw new ArgumentNullException(nameof(containers));

            _Settings = settings;
            _Logging = logging;
            _ContainerMgr = containers;
        }

        #endregion

        #region Public-Methods

        public ContainerMetadata Enumerate(
            RequestMetadata md,
            Container container, 
            int? indexStart, 
            int? maxResults,
            string orderByClause)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
              
            EnumerationFilter filter = new EnumerationFilter();
            filter.CreatedAfter = md.Params.CreatedAfter;
            filter.CreatedBefore = md.Params.CreatedBefore;
            filter.LastAccessAfter = md.Params.LastAccessAfter;
            filter.LastAccessBefore = md.Params.LastAccessBefore;
            filter.UpdatedAfter = md.Params.UpdatedAfter;
            filter.UpdatedBefore = md.Params.UpdatedBefore;
            filter.Md5 = md.Params.Md5;
            filter.ContentType = md.Params.ContentType;
            filter.Tags = Common.CsvToStringList(md.Params.Tags);
            filter.SizeMin = md.Params.SizeMin;
            filter.SizeMax = md.Params.SizeMax;

            ContainerMetadata metadata = container.Enumerate(indexStart, maxResults, filter, orderByClause); 
            
            string logData =
               "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
               "User: " + md.Params.UserGuid;
            if (indexStart != null) logData += " Index: " + indexStart;
            if (maxResults != null) logData += " Results: " + maxResults;

            container.AddAuditLogEntry("[container]", AuditLogEntryType.Enumerate, logData, false);

            return metadata;
        }

        public void Delete(
            string userName, 
            string containerName)
        {
            if (String.IsNullOrEmpty(userName)) throw new ArgumentNullException(nameof(userName));
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));

            _ContainerMgr.Delete(userName, containerName, true);
        }

        public bool Exists(
            RequestMetadata md,
            string userName,
            string containerName
            )
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (String.IsNullOrEmpty(userName)) throw new ArgumentNullException(nameof(userName));
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));

            Container container = null;
            if (!_ContainerMgr.GetContainer(userName, containerName, out container))
            { 
                return false;
            }
            else
            {
                string logData =
                   "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                   "User: " + md.Params.UserGuid;
                container.AddAuditLogEntry("[container]", AuditLogEntryType.Exists, logData, false);
                return true;
            }
        }

        public void Create(
            RequestMetadata md,
            ContainerSettings settings
            )
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
              
            string baseDir = "";
            if (!String.IsNullOrEmpty(md.User.HomeDirectory))
            {
                baseDir = String.Copy(md.User.HomeDirectory);
                if (!baseDir.EndsWith("/")) baseDir += "/";
            }
            else
            {
                baseDir = String.Copy(_Settings.Storage.Directory);
                if (!baseDir.EndsWith("/")) baseDir += "/";
                baseDir += md.User.Guid + "/";
            }

            ContainerSettings currSettings = new ContainerSettings(md.User.Guid, settings.Name, baseDir); 
            if (settings != null)
            {
                currSettings.EnableAuditLogging = settings.EnableAuditLogging;
                currSettings.IsPublicRead = settings.IsPublicRead;
                currSettings.IsPublicWrite = settings.IsPublicWrite;
                currSettings.Replication = settings.Replication;
                currSettings.DatabaseDebug = settings.DatabaseDebug;
            }

            _ContainerMgr.Add(currSettings);
            return;
        }

        public void Update(
            RequestMetadata md,
            Container container,
            ContainerSettings settings
            )
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (settings == null) throw new ArgumentNullException(nameof(settings));

            string logData =
                "Source: " + md.Http.SourceIp + ":" + md.Http.SourcePort + " " +
                "User: " + md.Params.UserGuid + " " +
                "AuditLogging: " + settings.EnableAuditLogging + " " +
                "PublicRead: " + settings.IsPublicRead + " " +
                "PublicWrite: " + settings.IsPublicWrite;

            container.AddAuditLogEntry("[container]", AuditLogEntryType.Configuration, logData, true);

            if (settings.EnableAuditLogging) container.SetAuditLogging();
            else container.UnsetAuditLogging();

            if (settings.IsPublicRead) container.SetPublicRead();
            else container.UnsetPublicRead();

            if (settings.IsPublicWrite) container.SetPublicWrite();
            else container.UnsetPublicWrite();

            container.SetReplicationMode(settings.Replication);
            return;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
