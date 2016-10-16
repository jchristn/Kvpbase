using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        static HttpResponse UserApiHandler(RequestMetadata md)
        {
            #region Check-for-Metadata-Request
            
            bool reqMetadata = Common.IsTrue(md.CurrentHttpRequest.RetrieveHeaderValue("request_metadata"));
            if (reqMetadata)
            {
                return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", Common.SerializeJson(md), true);
            }

            #endregion

            #region Process
            
            switch (md.CurrentHttpRequest.Method.ToLower())
            {
                case "get":
                    #region get

                    if ((String.Compare(md.CurrentHttpRequest.RawUrlWithoutQuery, "/" + md.CurrentUserMaster.Guid) == 0) ||
                        (md.CurrentHttpRequest.RawUrlWithoutQuery.StartsWith("/" + md.CurrentUserMaster.Guid + "/")))
                    {
                        return GetHandler(md);
                    }

                    break;

                    #endregion

                case "put":
                    #region put
                        
                    if ((String.Compare(md.CurrentHttpRequest.RawUrlWithoutQuery, "/" + md.CurrentUserMaster.Guid) == 0) ||
                        (md.CurrentHttpRequest.RawUrlWithoutQuery.StartsWith("/" + md.CurrentUserMaster.Guid + "/")))
                    {
                        return PutHandler(md);
                    }

                    break;

                    #endregion

                case "post":
                    #region post
                        
                    if (WatsonCommon.UrlEqual(md.CurrentHttpRequest.RawUrlWithoutQuery, "/" + md.CurrentUserMaster.Guid + "/search", false))
                    {
                        ObjectHandler oh = new ObjectHandler(CurrentSettings, CurrentTopology, CurrentNode, Users, LockManager, Maintenance, EncryptionManager, Logging, Logger);
                        return oh.ObjectSearch(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrentHttpRequest.RawUrlWithoutQuery, "/" + md.CurrentUserMaster.Guid + "/rename", false))
                    {
                        return PostRename(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrentHttpRequest.RawUrlWithoutQuery, "/" + md.CurrentUserMaster.Guid + "/move", false))
                    {
                        return PostMove(md);
                    }

                    if ((String.Compare(md.CurrentHttpRequest.RawUrlWithoutQuery, "/" + md.CurrentUserMaster.Guid) == 0) ||
                        (md.CurrentHttpRequest.RawUrlWithoutQuery.StartsWith("/" + md.CurrentUserMaster.Guid + "/")))
                    {
                        return PostHandler(md);
                    }

                    break;

                    #endregion

                case "delete":
                    #region delete
                        
                    if (md.CurrentHttpRequest.RawUrlWithoutQuery.StartsWith("/" + md.CurrentUserMaster.Guid + "/"))
                    {
                        return DeleteHandler(md);
                    }

                    break;

                    #endregion

                case "head":
                    #region head
                        
                    if ((String.Compare(md.CurrentHttpRequest.RawUrlWithoutQuery, "/" + md.CurrentUserMaster.Guid) == 0) ||
                        (md.CurrentHttpRequest.RawUrlWithoutQuery.StartsWith("/" + md.CurrentUserMaster.Guid + "/")))
                    {
                        return HeadHandler(md);
                    }

                    break;

                    #endregion

                default:
                    #region default
                        
                    Logging.Log(LoggingModule.Severity.Warn, "UserApiHandler unknown HTTP method '" + md.CurrentHttpRequest.Method + "'");
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unsupported method.", null).ToJson(), true);

                    #endregion
            }

            #endregion

            #region Unknown-URL

            Logging.Log(LoggingModule.Severity.Warn, "UserApiHandler unknown URL " + md.CurrentHttpRequest.RawUrlWithoutQuery);
            return new HttpResponse(md.CurrentHttpRequest, false, 404, null, "application/json",
                new ErrorResponse(2, 404, "Unknown endpoint.", null).ToJson(), true);

            #endregion
        }
    }
}