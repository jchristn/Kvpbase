using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Containers;
using Kvpbase.Classes;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static async Task HttpGetContainers(RequestMetadata md)
        {
            string header = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";

            #region Validate-Authentication

            if (md.User == null)
            {
                _Logging.Warn(header + "HttpGetContainers no authentication material");
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }

            #endregion

            #region Validate-Request
             
            if (!md.Params.UserGuid.ToLower().Equals(md.User.GUID.ToLower()))
            {
                _Logging.Warn(header + "HttpGetContainers user " + md.User.GUID + " attempting to retrieve container list for user " + md.Params.UserGuid);
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }

            #endregion

            #region Retrieve-and-Respond

            List<Container> containers = _ContainerMgr.GetContainersByUser(md.Params.UserGuid);
            md.Http.Response.StatusCode = 200;
            md.Http.Response.ContentType = "application/json";
            await md.Http.Response.Send(Common.SerializeJson(containers, true));
            return;

            #endregion 
        }
    }
}