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
             
            ErrorCode error = ErrorCode.None;

            if (md.User == null || !(md.User.GUID.ToLower().Equals(md.Params.UserGUID.ToLower())))
            {
                _Logging.Warn(header + "HttpDeleteObject unauthorized unauthenticated write attempt to object " + md.Params.UserGUID + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }

            if (!md.Perm.DeleteObject)
            {
                _Logging.Warn(header + "HttpDeleteObject unauthorized delete attempt to object " + md.Params.UserGUID + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }

            ContainerClient client = _ContainerMgr.GetContainerClient(md.Params.UserGUID, md.Params.ContainerName);
            if (client == null)
            { 
                _Logging.Warn(header + "HttpDeleteObject unable to find container " + md.Params.UserGUID + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }
              
            if (!client.Exists(md.Params.ObjectKey))
            {
                _Logging.Warn(header + "HttpDeleteObject object " + md.Params.UserGUID + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + " does not exist");
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }

            if (md.Params.Keys)
            {
                _ObjectHandler.WriteKeyValues(md, client, null, out error);
                md.Http.Response.StatusCode = 204;
                await md.Http.Response.Send();
                return;
            }
            else if (md.Params.ReadLock || md.Params.WriteLock)
            {
                if (!_ObjectHandler.Unlock(md, client, out error))
                {
                    _Logging.Warn(header + "HttpDeleteObject unable to remove lock for " + md.Params.UserGUID + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + ": " + error.ToString());

                    int statusCode = 0;
                    int id = 0;
                    ContainerClient.HttpStatusFromErrorCode(error, out statusCode, out id);

                    md.Http.Response.StatusCode = statusCode;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(id, statusCode, "Unable to write lock.", error), true));
                    return;
                }
                else
                {
                    md.Http.Response.StatusCode = 204;
                    await md.Http.Response.Send();
                    return;
                } 
            }
            else
            {
                #region Delete-Object

                if (!_ObjectHandler.Delete(md, client, out error))
                {
                    _Logging.Warn(header + "HttpDeleteObject unable to delete object " + md.Params.UserGUID + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + ": " + error.ToString());
                    md.Http.Response.StatusCode = 500;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(4, 500, "Unable to delete object.", error), true));
                    return;
                }
                else
                {
                    _Logging.Debug(header + "HttpDeleteObject deleted object " + md.Params.UserGUID + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                    md.Http.Response.StatusCode = 204;
                    await md.Http.Response.Send();
                    return;
                }

                #endregion
            }
        }
    }
}