using System;
using System.Collections.Generic;
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
        public static HttpResponse HttpPutObject(RequestMetadata md)
        { 
            #region Retrieve-Container

            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                List<Node> nodes = new List<Node>();
                if (!_OutboundMessageHandler.FindContainerOwners(md, out nodes))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, 404, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(5, 404, "Unknown user or container.", null), true)));
                }
                else
                {
                    string redirectUrl = null;
                    HttpResponse redirectRest = _OutboundMessageHandler.BuildRedirectResponse(md, nodes[0], out redirectUrl);
                    _Logging.Log(LoggingModule.Severity.Debug, "HttpPutObject redirecting container " + md.Params.UserGuid + "/" + md.Params.Container + " to " + redirectUrl);
                    return redirectRest;
                }
            }

            bool isPublicWrite = currContainer.IsPublicWrite();

            #endregion

            #region Authenticate-and-Authorize

            if (!isPublicWrite)
            {
                if (md.User == null || !(md.User.Guid.ToLower().Equals(md.Params.UserGuid.ToLower())))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unauthorized unauthenticated write attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                    return new HttpResponse(md.Http, 401, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                }
            }

            if (md.Perm != null)
            {
                if (!md.Perm.WriteObject)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unauthorized write attempt to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                    return new HttpResponse(md.Http, 401, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                }
            }

            #endregion

            #region Check-if-Object-Exists

            if (!currContainer.Exists(md.Params.ObjectKey))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " does not exists");
                return new HttpResponse(md.Http, 404, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(5, 404, "Object does not exist.", null), true)));
            }

            #endregion
            
            #region Process

            ErrorCode error;
            bool cleanupRequired = false;

            if (!String.IsNullOrEmpty(md.Params.Rename))
            {
                #region Rename

                try
                {
                    if (!_ObjectHandler.Rename(md, currContainer, md.Params.ObjectKey, md.Params.Rename, out error))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unable to rename object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " to " + md.Params.Rename + ": " + error.ToString());
                        cleanupRequired = true;

                        int statusCode = 0;
                        int id = 0;
                        Helper.StatusFromContainerErrorCode(error, out statusCode, out id);

                        return new HttpResponse(md.Http, statusCode, null, "application/json",
                            Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(id, statusCode, "Unable to rename object.", error), true)));
                    }
                    else
                    {
                        if (!_OutboundMessageHandler.ObjectRename(md, currContainer.Settings))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unable to replicate operation to one or more nodes");
                            cleanupRequired = true;

                            return new HttpResponse(md.Http, 500, null, "application/json",
                                Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(10, 500, null, null), true))); 
                        }

                        return new HttpResponse(md.Http, 200, null, null, null);
                    }
                }
                finally
                {
                    if (cleanupRequired)
                    {
                        #region Rename-to-Orginal

                        _ObjectHandler.Rename(md, currContainer, md.Params.Rename, md.Params.ObjectKey, out error);

                        string renameKey = String.Copy(md.Params.Rename);
                        string originalKey = String.Copy(md.Params.ObjectKey);

                        md.Params.ObjectKey = renameKey;
                        md.Params.Rename = originalKey;

                        _OutboundMessageHandler.ObjectRename(md, currContainer.Settings);

                        #endregion
                    }
                }

                #endregion
            }
            else if (md.Params.Index != null)
            {
                #region Range-Write

                #region Verify-Transfer-Size

                if (md.Http.ContentLength > _Settings.Server.MaxTransferSize ||
                    (md.Http.Data != null && md.Http.Data.Length > _Settings.Server.MaxTransferSize)
                    )
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject transfer size too large (count requested: " + md.Params.Count + ")");
                    return new HttpResponse(md.Http, 413, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(11, 413, null, null), true)));
                }

                #endregion

                #region Retrieve-Original-Data

                // md.Http.Data = Common.StreamToBytes(md.Http.DataStream); 
                byte[] originalData = null;
                string originalContentType = null;
                if (!_ObjectHandler.Read(md, currContainer, md.Params.ObjectKey, Convert.ToInt32(md.Params.Index), Convert.ToInt32(md.Http.ContentLength), out originalContentType, out originalData, out error))
                {
                    if (error == ErrorCode.OutOfRange)
                    {
                        // continue, simply appending the data
                        originalData = new byte[md.Http.ContentLength];
                        for (int i = 0; i < originalData.Length; i++)
                        {
                            originalData[i] = 0x00;
                        }
                    }
                    else
                    { 
                        _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unable to retrieve original data from object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + ": " + error.ToString());
                        return new HttpResponse(md.Http, 500, null, "application/json",
                            Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(4, 500, "Unable to retrieve original data.", null), true)));
                    }
                }

                #endregion

                #region Perform-Update

                try
                {
                    if (!_ObjectHandler.WriteRange(md, currContainer, md.Params.ObjectKey, Convert.ToInt64(md.Params.Index), md.Http.ContentLength, md.Http.DataStream, out error))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unable to write range to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + ": " + error.ToString());
                        cleanupRequired = true;

                        int statusCode = 0;
                        int id = 0;
                        Helper.StatusFromContainerErrorCode(error, out statusCode, out id);

                        return new HttpResponse(md.Http, statusCode, null, "application/json",
                            Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(id, statusCode, "Unable to write range to object.", error), true)));
                    }
                    else
                    { 
                        if (!_OutboundMessageHandler.ObjectWriteRange(md, currContainer.Settings))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unable to replicate operation to one or more nodes");
                            cleanupRequired = true;

                            return new HttpResponse(md.Http, 500, null, "application/json",
                                Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(10, 500, null, null), true)));
                        }

                        return new HttpResponse(md.Http, 200, null, null, null);
                    }
                }
                finally
                {
                    if (cleanupRequired)
                    {
                        _ObjectHandler.WriteRange(md, currContainer, md.Params.ObjectKey, Convert.ToInt64(md.Params.Index), originalData, out error);

                        md.Http.Data = new byte[originalData.Length];
                        Buffer.BlockCopy(originalData, 0, md.Http.Data, 0, originalData.Length);

                        _OutboundMessageHandler.ObjectWriteRange(md, currContainer.Settings);
                    }
                }

                #endregion

                #endregion
            }
            else if (!String.IsNullOrEmpty(md.Params.Tags))
            {
                #region Tags

                #region Retrieve-Original-Object-Metadata

                ObjectMetadata originalMetadata = null;
                if (!currContainer.ReadObjectMetadata(md.Params.ObjectKey, out originalMetadata))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unable to read original metadata for tag rewrite for object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                    return new HttpResponse(md.Http, 404, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(5, 404, "Object not found.", null), true)));
                }

                #endregion

                #region Update-Tags

                try
                {
                    if (!currContainer.WriteObjectTags(md.Params.ObjectKey, md.Params.Tags, out error))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unable to write tags to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + ": " + error.ToString());
                        cleanupRequired = true;

                        int statusCode = 0;
                        int id = 0;
                        Helper.StatusFromContainerErrorCode(error, out statusCode, out id);

                        return new HttpResponse(md.Http, statusCode, null, "application/json",
                            Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(id, statusCode, "Unable to write tags to object.", error), true)));
                    }
                    else
                    {
                        if (!_OutboundMessageHandler.ObjectWriteTags(md, currContainer.Settings))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject unable to replicate operation to one or more nodes");
                            cleanupRequired = true;

                            return new HttpResponse(md.Http, 500, null, "application/json",
                                Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(10, 500, null, null), true)));
                        }

                        return new HttpResponse(md.Http, 200, null, null, null);
                    }
                }
                finally
                {
                    if (cleanupRequired)
                    {
                        _ObjectHandler.WriteTags(md, currContainer, md.Params.ObjectKey, originalMetadata.Tags, out error);
                        _OutboundMessageHandler.ObjectWriteTags(md, currContainer.Settings);
                    }
                }

                #endregion

                #endregion
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpPutObject request query does not contain _index, _rename, or _tags"); 
                return new HttpResponse(md.Http, 400, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(2, 400, "Querystring must contain values for '_index', '_rename', or '_tags'.", null), true)));
            }

            #endregion 
        }
    }
}