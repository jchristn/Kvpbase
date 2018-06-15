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
        public static HttpResponse HttpGetObject(RequestMetadata md)
        { 
            #region Retrieve-Container
             
            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                List<Node> nodes = new List<Node>();
                if (!_OutboundMessageHandler.FindContainerOwners(md, out nodes))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpGetObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Unknown user or container.", null), true);
                }
                else
                {
                    string redirectUrl = null;
                    HttpResponse redirectRest = _OutboundMessageHandler.BuildRedirectResponse(md, nodes[0], out redirectUrl);
                    _Logging.Log(LoggingModule.Severity.Debug, "HttpGetObject redirecting container " + md.Params.UserGuid + "/" + md.Params.Container + " to " + redirectUrl);
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
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpGetObject unauthorized unauthenticated access attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                    return new HttpResponse(md.Http, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Unauthorized.", null), true);
                }
            }

            if (md.Perm != null)
            {
                if (!md.Perm.ReadObject)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpGetObject unauthorized access attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                    return new HttpResponse(md.Http, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Unauthorized.", null), true);
                }
            }

            #endregion

            #region Retrieve-Metadata

            ObjectMetadata metadata = null;
            if (!currContainer.ReadObjectMetadata(md.Params.ObjectKey, out metadata))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpGetObject unable to retrieve metadata for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);

                int statusCode = 0;
                int id = 0;
                Helper.StatusFromContainerErrorCode(ErrorCode.NotFound, out statusCode, out id);

                return new HttpResponse(md.Http, false, statusCode, null, "application/json",
                    new ErrorResponse(id, statusCode, null, ErrorCode.NotFound), true);
            }

            if (md.Params.Metadata)
            {
                return new HttpResponse(md.Http, true, 200, null, "application/json", Common.SerializeJson(metadata, true), true);
            }

            #endregion

            #region Verify-Transfer-Size

            if (md.Params.Index != null && md.Params.Count != null)
            {
                if (Convert.ToInt32(md.Params.Count) > _Settings.Server.MaxTransferSize)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpGetObject transfer size too large (count requested: " + md.Params.Count + ")");
                    return new HttpResponse(md.Http, false, 413, null, "application/json",
                        new ErrorResponse(11, 413, null, null), true);
                }
            }
            else
            {
                if (metadata.ContentLength > _Settings.Server.MaxTransferSize)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpGetObject transfer size too large (content length: " + metadata.ContentLength + ")");
                    return new HttpResponse(md.Http, false, 413, null, "application/json",
                        new ErrorResponse(11, 413, null, null), true);
                }
            }

            #endregion

            #region Retrieve-and-Return

            int? index = null;
            if (md.Params.Index != null) index = Convert.ToInt32(md.Params.Index);

            int? count = null;
            if (md.Params.Count != null) count = Convert.ToInt32(md.Params.Count);

            string contentType = null;
            byte[] data = null;
            ErrorCode error;

            if (!_ObjectHandler.Read(md, currContainer, md.Params.ObjectKey, index, count, out contentType, out data, out error))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpGetObject unable to find object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);

                int statusCode = 0;
                int id = 0;
                Helper.StatusFromContainerErrorCode(error, out statusCode, out id);

                return new HttpResponse(md.Http, false, statusCode, null, "application/json",
                    new ErrorResponse(id, statusCode, null, error), true);
            }
            else
            {
                return new HttpResponse(md.Http, true, 200, null, contentType, data, true);
            }
                 
            #endregion 
        }
    }
}