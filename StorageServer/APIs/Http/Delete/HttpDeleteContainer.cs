using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpDeleteContainer(RequestMetadata md)
        {
            #region Validate-Authentication

            if (md.User == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpDeleteContainer no authentication material");
                return new HttpResponse(md.Http, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Unauthorized.", null), true);
            }

            #endregion

            #region Validate-Request
             
            if (!md.Params.UserGuid.ToLower().Equals(md.User.Guid.ToLower()))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpDeleteContainer user " + md.User.Guid + " attempting to create container in user " + md.Params.UserGuid);
                return new HttpResponse(md.Http, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Unauthorized.", null), true);
            }

            if (!md.Perm.DeleteContainer)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpDeleteContainer unauthorized delete attempt to container " + md.Params.UserGuid + "/" + md.Params.Container);
                return new HttpResponse(md.Http, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Unauthorized.", null), true);
            }

            #endregion

            #region Retrieve-Container

            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpDeleteContainer container " + md.Params.UserGuid + "/" + md.Params.Container + " does not exist");
                return new HttpResponse(md.Http, false, 404, null, "application/json",
                    new ErrorResponse(5, 404, null, null), true);
            }

            #endregion

            #region Process

            if (md.Params.AuditLog)
            {
                currContainer.ClearAuditLog();
                _OutboundMessageHandler.ContainerClearAuditLog(md, currContainer.Settings);
                _Logging.Log(LoggingModule.Severity.Debug, "HttpDeleteContainer cleared audit log for container " + md.Params.UserGuid + "/" + md.Params.Container);
                return new HttpResponse(md.Http, true, 204, null, "application/json", null, true);
            }
            else
            {
                _ContainerHandler.Delete(md.Params.UserGuid, md.Params.Container);
                _OutboundMessageHandler.ContainerDelete(md, currContainer.Settings);
                _Logging.Log(LoggingModule.Severity.Debug, "HttpDeleteContainer deleted container " + md.Params.UserGuid + "/" + md.Params.Container);
                return new HttpResponse(md.Http, true, 204, null, "application/json", null, true);
            }

            #endregion
        }
    }
}