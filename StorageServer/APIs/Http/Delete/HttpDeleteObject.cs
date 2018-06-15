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
        public static HttpResponse HttpDeleteObject(RequestMetadata md)
        {
            #region Authenticate-and-Authorize

            if (md.User == null || !(md.User.Guid.ToLower().Equals(md.Params.UserGuid.ToLower())))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpDeleteObject unauthorized unauthenticated write attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                return new HttpResponse(md.Http, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Unauthorized.", null), true);
            }

            if (!md.Perm.DeleteObject)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpDeleteObject unauthorized delete attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                return new HttpResponse(md.Http, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Unauthorized.", null), true);
            }

            #endregion

            #region Retrieve-Container

            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                List<Node> nodes = new List<Node>();
                if (!_OutboundMessageHandler.FindContainerOwners(md, out nodes))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpDeleteObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Unknown user or container.", null), true);
                }
                else
                {
                    string redirectUrl = null;
                    HttpResponse redirectRest = _OutboundMessageHandler.BuildRedirectResponse(md, nodes[0], out redirectUrl);
                    _Logging.Log(LoggingModule.Severity.Debug, "HttpDeleteObject redirecting container " + md.Params.UserGuid + "/" + md.Params.Container + " to " + redirectUrl);
                    return redirectRest;
                }
            }
             
            #endregion

            #region Check-if-Object-Exists
             
            if (!currContainer.Exists(md.Params.ObjectKey))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpDeleteObject object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " does not exist");
                return new HttpResponse(md.Http, false, 404, null, "application/json",
                    new ErrorResponse(5, 404, null, null), true);
            }

            #endregion
             
            #region Delete-and-Respond

            ErrorCode error;
            if (!_ObjectHandler.Delete(md, currContainer, md.Params.ObjectKey, out error))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpDeleteObject unable to delete object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + ": " + error.ToString());
                return new HttpResponse(md.Http, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to delete object.", error), true);
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Debug, "HttpDeleteObject deleted object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                _OutboundMessageHandler.ObjectDelete(md, currContainer.Settings);
                return new HttpResponse(md.Http, true, 204, null, null, null, true);
            } 
            
            #endregion 
        }
    }
}