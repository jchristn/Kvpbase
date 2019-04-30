using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpHeadObject(RequestMetadata md)
        { 
            #region Retrieve-Container

            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                List<Node> nodes = new List<Node>();
                if (!_OutboundMessageHandler.FindContainerOwners(md, out nodes))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpHeadObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, 404, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(5, 404, "Unknown user or container.", null), true)));
                }
                else
                {
                    string redirectUrl = null;
                    HttpResponse redirectRest = _OutboundMessageHandler.BuildRedirectResponse(md, nodes[0], out redirectUrl);
                    _Logging.Log(LoggingModule.Severity.Debug, "HttpHeadObject redirecting container " + md.Params.UserGuid + "/" + md.Params.Container + " to " + redirectUrl);
                    return redirectRest;
                }
            }
            bool isPublicRead = currContainer.IsPublicRead();

            #endregion

            #region Authenticate-and-Authorize

            if (!isPublicRead)
            {
                if (md.User == null || !(md.User.Guid.ToLower().Equals(md.Params.UserGuid.ToLower())))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpHeadObject unauthorized unauthenticated access attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                    return new HttpResponse(md.Http, 401, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                }
            }

            if (md.Perm != null)
            {
                if (!md.Perm.ReadObject)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpHeadObject unauthorized access attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                    return new HttpResponse(md.Http, 401, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                }
            }

            #endregion
              
            #region Retrieve-and-Return
             
            if (!_ObjectHandler.Exists(md, currContainer, md.Params.ObjectKey))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpHeadObject unable to find object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                return new HttpResponse(md.Http, 404, null, null, null); 
            }
            else
            { 
                return new HttpResponse(md.Http, 200, null, null, null);
            } 

            #endregion 
        }
    }
}