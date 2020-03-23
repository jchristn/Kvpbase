using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver; 
using Kvpbase.StorageServer.Classes;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer
{
    public partial class Program
    {
        internal static async Task HttpGetObject(RequestMetadata md)
        {
            string header = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";
             
            ContainerClient client = null;
            if (!_ContainerMgr.GetContainerClient(md.Params.UserGuid, md.Params.ContainerName, out client))
            { 
                _Logging.Warn(header + "HttpGetObject unable to find container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }
              
            if (!client.Container.IsPublicRead)
            {
                if (md.User == null || !(md.User.GUID.ToLower().Equals(md.Params.UserGuid.ToLower())))
                {
                    _Logging.Warn(header + "HttpGetObject unauthorized unauthenticated access attempt to object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
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
                    _Logging.Warn(header + "HttpGetObject unauthorized access attempt to object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                    md.Http.Response.StatusCode = 401;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                    return;
                }
            }
             
            ErrorCode error;
            ObjectMetadata metadata = null;
            if (!client.ReadObjectMetadata(md.Params.ObjectKey, out metadata))
            {
                _Logging.Warn(header + "HttpGetObject unable to retrieve metadata for " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);

                int statusCode = 0;
                int id = 0;
                ContainerClient.HttpStatusFromErrorCode(ErrorCode.NotFound, out statusCode, out id);

                md.Http.Response.StatusCode = statusCode;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(id, statusCode, null, ErrorCode.NotFound), true));
                return;
            }

            if (md.Params.Metadata)
            {
                md.Http.Response.StatusCode = 200;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(metadata, true));
                return;
            }
            else if (md.Params.Keys)
            {
                Dictionary<string, string> vals = new Dictionary<string, string>();
                if (!_ObjectHandler.ReadKeyValues(md, client, md.Params.ObjectKey, out vals, out error))
                {
                    int statusCode = 0;
                    int id = 0;
                    ContainerClient.HttpStatusFromErrorCode(error, out statusCode, out id);

                    _Logging.Warn(header + "HttpGetObject unable to read key-values for " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);

                    md.Http.Response.StatusCode = statusCode;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(id, statusCode, null, error), true));
                    return;
                }

                md.Http.Response.StatusCode = 200;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(vals, true));
                return;
            } 
             
            if (md.Params.Index != null && md.Params.Count != null)
            {
                if (Convert.ToInt32(md.Params.Count) > _Settings.Server.MaxTransferSize)
                {
                    _Logging.Warn(header + "HttpGetObject transfer size too large (count requested: " + md.Params.Count + ")");

                    md.Http.Response.StatusCode = 413;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(11, 413, null, null), true));
                    return; 
                }
            }
            else
            {
                if (metadata.ContentLength > _Settings.Server.MaxTransferSize)
                {
                    _Logging.Warn(header + "HttpGetObject transfer size too large (content length: " + metadata.ContentLength + ")");
                    md.Http.Response.StatusCode = 413;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(11, 413, null, null), true));
                    return;
                }
            }
             
            int? index = null;
            if (md.Params.Index != null) index = Convert.ToInt32(md.Params.Index);

            int? count = null;
            if (md.Params.Count != null) count = Convert.ToInt32(md.Params.Count);

            string contentType = null;
            long contentLength = 0;
            Stream stream = null;
             
            if (!_ObjectHandler.Read(md, client, md.Params.ObjectKey, index, count, out contentType, out contentLength, out stream, out error))
            {
                int statusCode = 0;
                int id = 0;
                ContainerClient.HttpStatusFromErrorCode(error, out statusCode, out id);

                _Logging.Warn(header + "HttpGetObject unable to read object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);

                md.Http.Response.StatusCode = statusCode;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(id, statusCode, null, error), true));
                return;
            }
            else
            {
                md.Http.Response.StatusCode = 200;
                md.Http.Response.ContentType = contentType;
                await md.Http.Response.Send(contentLength, stream);
                return;
            } 
        }
    }
}