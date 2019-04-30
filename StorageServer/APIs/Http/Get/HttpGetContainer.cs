using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpGetContainer(RequestMetadata md)
        {  
            #region Retrieve-Container
            
            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                List<Node> nodes = new List<Node>();
                if (!_OutboundMessageHandler.FindContainerOwners(md, out nodes))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpGetContainer unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, 404, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(5, 404, "Unknown user or container.", null), true)));
                }
                else
                {
                    string redirectUrl = null;
                    HttpResponse redirectRest = _OutboundMessageHandler.BuildRedirectResponse(md, nodes[0], out redirectUrl);
                    _Logging.Log(LoggingModule.Severity.Debug, "HttpGetContainer redirecting container " + md.Params.UserGuid + "/" + md.Params.Container + " to " + redirectUrl);
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
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpGetContainer unauthorized unauthenticated access attempt to container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, 401, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                }
            }
             
            if (md.Perm != null)
            {
                if (!md.Perm.ReadContainer)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpGetContainer unauthorized access attempt to container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, 401, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                }
            }

            #endregion

            #region Retrieve-Settings

            ContainerSettings settings = null;
            if (!_ContainerMgr.GetContainerSettings(md.Params.UserGuid, md.Params.Container, out settings))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpGetContainer unable to retrieve settings for " + md.Params.UserGuid + "/" + md.Params.Container);
                return new HttpResponse(md.Http, 500, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(4, 500, null, null), true)));
            }

            if (md.Params.Config)
            {
                return new HttpResponse(md.Http, 200, null, "application/json", Encoding.UTF8.GetBytes(Common.SerializeJson(settings, true)));
            }

            #endregion

            #region Enumerate-and-Return

            int? index = null;
            if (md.Params.Index != null) index = Convert.ToInt32(md.Params.Index);

            int? count = null;
            if (md.Params.Count != null) count = Convert.ToInt32(md.Params.Count);
             
            ContainerMetadata meta = _ContainerHandler.Enumerate(md, currContainer, index, count, md.Params.OrderBy);
             
            return new HttpResponse(md.Http, 200, null, "application/json",
                Encoding.UTF8.GetBytes(Common.SerializeJson(meta, true)));

            #endregion 
        }
    }
}