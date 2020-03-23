using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;
using Kvpbase.StorageServer.Classes; 

namespace Kvpbase.StorageServer
{
    public partial class Program
    {
        internal static async Task HttpDeleteObject(RequestMetadata md)
        {
            string header = _Header + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";

            if (md.User == null || !(md.User.GUID.ToLower().Equals(md.Params.UserGuid.ToLower())))
            {
                _Logging.Warn(header + "HttpDeleteObject unauthorized unauthenticated write attempt to object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }

            if (!md.Perm.DeleteObject)
            {
                _Logging.Warn(header + "HttpDeleteObject unauthorized delete attempt to object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }
             
            ContainerClient client = null;
            if (!_ContainerMgr.GetContainerClient(md.Params.UserGuid, md.Params.ContainerName, out client))
            { 
                _Logging.Warn(header + "HttpDeleteObject unable to find container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }
              
            if (!client.Exists(md.Params.ObjectKey))
            {
                _Logging.Warn(header + "HttpDeleteObject object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + " does not exist");
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }

            ErrorCode error = ErrorCode.None;
            if (md.Params.Keys)
            {
                _ObjectHandler.WriteKeyValues(md, client, md.Params.ObjectKey, null, out error);
                md.Http.Response.StatusCode = 204;
                await md.Http.Response.Send();
                return;
            }

            if (!_ObjectHandler.Delete(md, client, md.Params.ObjectKey, out error))
            {
                _Logging.Warn(header + "HttpDeleteObject unable to delete object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + ": " + error.ToString());
                md.Http.Response.StatusCode = 500;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(4, 500, "Unable to delete object.", error), true));
                return;
            }
            else
            {
                _Logging.Debug(header + "HttpDeleteObject deleted object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                md.Http.Response.StatusCode = 204;
                await md.Http.Response.Send();
                return;
            }  
        }
    }
}