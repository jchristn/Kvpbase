using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpPostObject(RequestMetadata md)
        {
            bool cleanupRequired = false;  
            Container currContainer = null;
            ErrorCode error;

            string guid = null; 
            Stream stream = null;
            long contentLength = 0;

            try
            { 
                #region Retrieve-Container
                 
                if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, 404, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(5, 404, "Unknown user or container.", null), true)));
                }

                bool isPublicWrite = currContainer.IsPublicWrite();

                #endregion

                #region Authenticate-and-Authorize

                if (!isPublicWrite)
                {
                    if (md.User == null || !(md.User.Guid.ToLower().Equals(md.Params.UserGuid.ToLower())))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unauthorized unauthenticated write attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                        return new HttpResponse(md.Http, 401, null, "application/json",
                            Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                    }
                }

                if (md.Perm != null)
                {
                    if (!md.Perm.WriteObject)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unauthorized write attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                        return new HttpResponse(md.Http, 401, null, "application/json",
                            Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                    }
                }

                #endregion

                #region Check-if-Object-Exists

                if (currContainer.Exists(md.Params.ObjectKey))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " already exists");
                    return new HttpResponse(md.Http, 409, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(7, 409, null, null), true)));
                }

                #endregion

                #region Verify-Transfer-Size

                if (_Settings.Server.MaxObjectSize > 0 && md.Http.ContentLength > _Settings.Server.MaxObjectSize)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject object size too large (" + md.Http.ContentLength + " bytes)");
                    return new HttpResponse(md.Http, 413, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(11, 413, null, null), true)));
                }

                #endregion

                #region Write-and-Return
                 
                if (!_TempFilesMgr.Add(md.Http.ContentLength, md.Http.DataStream, out guid))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unable to write temporary file for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                    return new HttpResponse(md.Http, 500, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(4, 500, "Unable to write temporary file.", null), true)));
                }

                if (!_TempFilesMgr.GetStream(guid, out contentLength, out stream))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unable to attach stream for for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                    return new HttpResponse(md.Http, 500, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(4, 500, "Unable to attach stream.", null), true)));
                }
                 
                if (!_ObjectHandler.Create(md, currContainer, md.Params.ObjectKey, md.Http.ContentType, contentLength, stream, out error))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unable to write object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + ": " + error.ToString());

                    int statusCode = 0;
                    int id = 0;
                    Helper.StatusFromContainerErrorCode(error, out statusCode, out id);

                    return new HttpResponse(md.Http, statusCode, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(id, statusCode, "Unable to write object.", error), true)));
                }
                else
                { 
                    if (stream.CanSeek) stream.Seek(0, SeekOrigin.Begin);
                    if (!_OutboundMessageHandler.ObjectCreate(md, currContainer.Settings, contentLength, stream))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unable to replicate operation to one or more nodes");
                        cleanupRequired = true;

                        return new HttpResponse(md.Http, 500, null, "application/json",
                            Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(10, 500, null, null), true)));
                    }

                    return new HttpResponse(md.Http, 201, null);
                }

                #endregion
            }
            finally
            {
                if (cleanupRequired)
                {
                    _ObjectHandler.Delete(md, currContainer, md.Params.ObjectKey, out error);
                    _OutboundMessageHandler.ObjectDelete(md, currContainer.Settings);
                }

                if (stream != null)
                {
                    stream.Close();
                    stream.Dispose();
                }

                if (!String.IsNullOrEmpty(guid))
                {
                    _TempFilesMgr.Delete(guid);
                }
            }
        }
    }
}