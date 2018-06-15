using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpPostObject(RequestMetadata md)
        {
            bool cleanupRequired = false;  
            Container currContainer = null;
            ErrorCode error;

            try
            { 
                #region Retrieve-Container
                 
                if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Unknown user or container.", null), true);
                }

                bool isPublicWrite = currContainer.IsPublicWrite();

                #endregion

                #region Authenticate-and-Authorize

                if (!isPublicWrite)
                {
                    if (md.User == null || !(md.User.Guid.ToLower().Equals(md.Params.UserGuid.ToLower())))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unauthorized unauthenticated write attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                        return new HttpResponse(md.Http, false, 401, null, "application/json",
                            new ErrorResponse(3, 401, "Unauthorized.", null), true);
                    }
                }

                if (md.Perm != null)
                {
                    if (!md.Perm.WriteObject)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unauthorized write attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                        return new HttpResponse(md.Http, false, 401, null, "application/json",
                            new ErrorResponse(3, 401, "Unauthorized.", null), true);
                    }
                }

                #endregion

                #region Check-if-Object-Exists

                if (currContainer.Exists(md.Params.ObjectKey))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " already exists");
                    return new HttpResponse(md.Http, false, 409, null, "application/json",
                        new ErrorResponse(7, 409, null, null), true);
                }

                #endregion

                #region Verify-Transfer-Size

                if (md.Http.ContentLength > _Settings.Server.MaxTransferSize ||
                    (md.Http.Data != null && md.Http.Data.Length > _Settings.Server.MaxTransferSize)
                    )
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject transfer size too large (count requested: " + md.Params.Count + ")");
                    return new HttpResponse(md.Http, false, 413, null, "application/json",
                        new ErrorResponse(11, 413, null, null), true);
                }

                #endregion

                #region Write-and-Return

                if (!_ObjectHandler.Create(md, currContainer, md.Params.ObjectKey, md.Http.ContentType, md.Http.Data, out error))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unable to write object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + ": " + error.ToString());

                    int statusCode = 0;
                    int id = 0;
                    Helper.StatusFromContainerErrorCode(error, out statusCode, out id);

                    return new HttpResponse(md.Http, false, statusCode, null, "application/json",
                        new ErrorResponse(id, statusCode, "Unable to write object.", error), true);
                }
                else
                {
                    if (!_OutboundMessageHandler.ObjectCreate(md, currContainer.Settings))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HttpPostObject unable to replicate operation to one or more nodes");
                        cleanupRequired = true;

                        return new HttpResponse(md.Http, false, 500, null, "application/json",
                            new ErrorResponse(10, 500, null, null), true);
                    }

                    return new HttpResponse(md.Http, true, 201, null, null, null, true);
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
            }
        }
    }
}