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
        public static async Task HttpHeadObject(RequestMetadata md)
        {
            string header = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";

            #region Retrieve-Container

            ContainerClient client = null;
            if (!_ContainerMgr.GetContainerClient(md.Params.UserGuid, md.Params.ContainerName, out client))
            { 
                _Logging.Warn(header + "HttpHeadObject unable to find container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }

            #endregion

            #region Authenticate-and-Authorize

            if (!client.Container.IsPublicRead)
            {
                if (md.User == null || !(md.User.GUID.ToLower().Equals(md.Params.UserGuid.ToLower())))
                {
                    _Logging.Warn(header + "HttpHeadObject unauthorized unauthenticated access attempt to object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                    md.Http.Response.StatusCode = 401;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                    return;
                }
            }

            if (md.Perm != null)
            {
                if (!md.Perm.ReadObject)
                {
                    _Logging.Warn(header + "HttpHeadObject unauthorized access attempt to object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                    md.Http.Response.StatusCode = 401;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                    return;
                }
            }

            #endregion
              
            #region Retrieve-and-Return
             
            if (!_ObjectHandler.Exists(md, client, md.Params.ObjectKey))
            {
                _Logging.Debug(header + "HttpHeadObject unable to find object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                md.Http.Response.StatusCode = 404;
                await md.Http.Response.Send();
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