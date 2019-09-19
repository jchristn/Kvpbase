using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Classes;
using Kvpbase.Containers;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static async Task HttpHeadContainer(RequestMetadata md)
        {
            string header = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";

            #region Retrieve-Container

            Container currContainer = _ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.ContainerName);
            if (currContainer == null)
            { 
                _Logging.Debug(header + "HttpHeadContainer unable to find container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }
            else
            {
                md.Http.Response.StatusCode = 200;
                await md.Http.Response.Send();
                return; 
            }
            
            #endregion
        }
    }
}