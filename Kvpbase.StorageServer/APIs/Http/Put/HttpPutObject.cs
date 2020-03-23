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
        internal static async Task HttpPutObject(RequestMetadata md)
        {
            string header = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";
             
            ContainerClient client = null;
            if (!_ContainerMgr.GetContainerClient(md.Params.UserGuid, md.Params.ContainerName, out client))
            { 
                _Logging.Warn(header + "HttpPutObject unable to find container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }
              
            if (!client.Container.IsPublicWrite)
            {
                if (md.User == null || !(md.User.GUID.ToLower().Equals(md.Params.UserGuid.ToLower())))
                {
                    _Logging.Warn(header + "HttpPutObject unauthorized unauthenticated write attempt to object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                    md.Http.Response.StatusCode = 401;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                    return;
                }
            }

            if (md.Perm != null)
            {
                if (!md.Perm.WriteObject)
                {
                    _Logging.Warn(header + "HttpPutObject unauthorized write attempt to object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                    md.Http.Response.StatusCode = 401;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                    return;
                }
            }
             
            if (!client.Exists(md.Params.ObjectKey))
            {
                _Logging.Warn(header + "HttpPutObject object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + " does not exists");
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }
             
            ErrorCode error;  

            if (!String.IsNullOrEmpty(md.Params.Rename))
            {
                #region Rename
                 
                if (!_ObjectHandler.Rename(md, client, md.Params.ObjectKey, md.Params.Rename, out error))
                {
                    _Logging.Warn(header + "HttpPutObject unable to rename object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + " to " + md.Params.Rename + ": " + error.ToString());

                    int statusCode = 0;
                    int id = 0;
                    ContainerClient.HttpStatusFromErrorCode(error, out statusCode, out id);

                    md.Http.Response.StatusCode = statusCode;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(id, statusCode, "Unable to rename.", error), true));
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
            else if (md.Params.Index != null)
            {
                #region Range-Write

                #region Verify-Transfer-Size

                if (md.Http.Request.ContentLength > _Settings.Server.MaxTransferSize ||
                    (md.Http.Request.Data != null && md.Http.Request.ContentLength > _Settings.Server.MaxTransferSize)
                    )
                {
                    _Logging.Warn(header + "HttpPutObject transfer size too large (count requested: " + md.Params.Count + ")");
                    md.Http.Response.StatusCode = 413;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(11, 413, null, null), true));
                    return;
                }

                #endregion

                #region Retrieve-Original-Data

                long originalContentLength = 0;
                Stream originalDataStream = null;
                byte[] originalData = null;
                string originalContentType = null;

                if (!_ObjectHandler.Read(
                    md, 
                    client, 
                    md.Params.ObjectKey, 
                    Convert.ToInt32(md.Params.Index), 
                    Convert.ToInt32(md.Http.Request.ContentLength), 
                    out originalContentType, 
                    out originalContentLength,
                    out originalDataStream, 
                    out error))
                {
                    if (error == ErrorCode.OutOfRange)
                    {
                        // continue, simply appending the data
                        originalData = new byte[md.Http.Request.ContentLength];
                        for (int i = 0; i < originalData.Length; i++)
                        {
                            originalData[i] = 0x00;
                        }
                    }
                    else
                    { 
                        _Logging.Warn(header + "HttpPutObject unable to retrieve original data from object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + ": " + error.ToString());
                        md.Http.Response.StatusCode = 500;
                        md.Http.Response.ContentType = "application/json";
                        await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(4, 500, "Unable to retrieve original data.", error), true));
                        return;
                    }
                }
                else
                {
                    if (originalContentLength > 0 && originalDataStream != null)
                    {
                        originalData = Common.StreamToBytes(originalDataStream);
                    }
                }

                #endregion

                #region Perform-Update
                 
                if (!_ObjectHandler.WriteRange(md, client, md.Params.ObjectKey, Convert.ToInt64(md.Params.Index), md.Http.Request.ContentLength, md.Http.Request.Data, out error))
                {
                    _Logging.Warn(header + "HttpPutObject unable to write range to object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + ": " + error.ToString());

                    int statusCode = 0;
                    int id = 0;
                    ContainerClient.HttpStatusFromErrorCode(error, out statusCode, out id);

                    md.Http.Response.StatusCode = statusCode;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(id, statusCode, "Unable to write range to object.", error), true));
                    return;
                }
                else
                {
                    md.Http.Response.StatusCode = 200;
                    await md.Http.Response.Send();
                    return;
                } 

                #endregion

                #endregion
            }
            else if (!String.IsNullOrEmpty(md.Params.Tags))
            {
                #region Tags

                #region Retrieve-Original-Object-Metadata

                ObjectMetadata originalMetadata = null;
                if (!client.ReadObjectMetadata(md.Params.ObjectKey, out originalMetadata))
                {
                    _Logging.Warn(header + "HttpPutObject unable to read original metadata for tag rewrite for object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey);
                    md.Http.Response.StatusCode = 404;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                    return;
                }

                #endregion

                #region Update-Tags
                 
                if (!client.WriteObjectTags(md.Params.ObjectKey, md.Params.Tags, out error))
                {
                    _Logging.Warn(header + "HttpPutObject unable to write tags to object " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + ": " + error.ToString());

                    int statusCode = 0;
                    int id = 0;
                    ContainerClient.HttpStatusFromErrorCode(error, out statusCode, out id);

                    md.Http.Response.StatusCode = statusCode;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(id, statusCode, "Unable to write tags to object.", error), true));
                    return;
                }
                else
                {
                    md.Http.Response.StatusCode = 200;
                    await md.Http.Response.Send();
                    return;
                } 

                #endregion

                #endregion
            }
            else if (md.Params.Keys)
            {
                #region Update-Keys

                Dictionary<string, string> keys = null;

                if (md.Http.Request.Data != null && md.Http.Request.ContentLength > 0)
                {
                    byte[] reqData = Common.StreamToBytes(md.Http.Request.Data);

                    try
                    {
                        keys = Common.DeserializeJson<Dictionary<string, string>>(reqData);
                    }
                    catch (Exception)
                    {
                        _Logging.Warn(header + "HttpPutContainer unable to deserialize request body");
                        md.Http.Response.StatusCode = 400;
                        md.Http.Response.ContentType = "application/json";
                        await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(9, 400, null, null), true));
                        return;
                    }
                }
                 
                if (!_ObjectHandler.WriteKeyValues(md, client, md.Params.ObjectKey, keys, out error))
                {
                    _Logging.Warn(header + "HttpPutObject unable to write keys to " + md.Params.UserGuid + "/" + md.Params.ContainerName + "/" + md.Params.ObjectKey + ": " + error.ToString());

                    int statusCode = 0;
                    int id = 0;
                    ContainerClient.HttpStatusFromErrorCode(error, out statusCode, out id);

                    md.Http.Response.StatusCode = statusCode;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(id, statusCode, "Unable to write keys.", error), true));
                    return;
                }
                else
                {
                    md.Http.Response.StatusCode = 201;
                    await md.Http.Response.Send();
                    return;
                }

                #endregion
            }
            else
            {
                _Logging.Warn(header + "HttpPutObject request query does not contain _index, _rename, or _tags");
                md.Http.Response.StatusCode = 400;
                md.Http.Response.ContentType = "application.json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(2, 400, "Querystring must contain values for '_index', '_rename', or '_tags'.", null), true));
                return; 
            } 
        }
    }
}