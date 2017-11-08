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
            
            bool reqMetadata = Common.IsTrue(md.CurrHttpReq.RetrieveHeaderValue("request_metadata"));
            if (reqMetadata)
            {
                return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(md), true);
            }

            #endregion

            #region Process
            
            switch (md.CurrHttpReq.Method.ToLower())
            {
                case "get":
                    #region get

                    if ((String.Compare(md.CurrHttpReq.RawUrlWithoutQuery, "/" + md.CurrUser.Guid) == 0) ||
                        (md.CurrHttpReq.RawUrlWithoutQuery.StartsWith("/" + md.CurrUser.Guid + "/")))
                    {
                        return GetHandler(md);
                    }

                    break;

                    #endregion

                case "put":
                    #region put
                        
                    if ((String.Compare(md.CurrHttpReq.RawUrlWithoutQuery, "/" + md.CurrUser.Guid) == 0) ||
                        (md.CurrHttpReq.RawUrlWithoutQuery.StartsWith("/" + md.CurrUser.Guid + "/")))
                    {
                        return PutHandler(md);
                    }

                    break;

                    #endregion

                case "post":
                    #region post
                        
                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/" + md.CurrUser.Guid + "/search", false))
                    {
                        return _Object.Search(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/" + md.CurrUser.Guid + "/rename", false))
                    {
                        return PostRename(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/" + md.CurrUser.Guid + "/move", false))
                    {
                        return PostMove(md);
                    }

                    if ((String.Compare(md.CurrHttpReq.RawUrlWithoutQuery, "/" + md.CurrUser.Guid) == 0) ||
                        (md.CurrHttpReq.RawUrlWithoutQuery.StartsWith("/" + md.CurrUser.Guid + "/")))
                    {
                        return PostHandler(md);
                    }

                    break;

                    #endregion

                case "delete":
                    #region delete
                        
                    if (md.CurrHttpReq.RawUrlWithoutQuery.StartsWith("/" + md.CurrUser.Guid + "/"))
                    {
                        return DeleteHandler(md);
                    }

                    break;

                    #endregion

                case "head":
                    #region head
                        
                    if ((String.Compare(md.CurrHttpReq.RawUrlWithoutQuery, "/" + md.CurrUser.Guid) == 0) ||
                        (md.CurrHttpReq.RawUrlWithoutQuery.StartsWith("/" + md.CurrUser.Guid + "/")))
                    {
                        return HeadHandler(md);
                    }

                    break;

                    #endregion

                default:
                    #region default
                        
                    _Logging.Log(LoggingModule.Severity.Warn, "UserApiHandler unknown HTTP method '" + md.CurrHttpReq.Method + "'");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unsupported method.", null).ToJson(), true);

                    #endregion
            }

            #endregion

            #region Unknown-URL

            _Logging.Log(LoggingModule.Severity.Warn, "UserApiHandler unknown URL " + md.CurrHttpReq.RawUrlWithoutQuery);
            return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                new ErrorResponse(2, 404, "Unknown endpoint.", null).ToJson(), true);

            #endregion
        }
    }
}