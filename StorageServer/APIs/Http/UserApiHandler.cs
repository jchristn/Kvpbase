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
        static HttpResponse UserApiHandler(RequestMetadata md)
        {  
            if (Common.IsTrue(md.Http.RetrieveHeaderValue("_metadata")))
            {
                Dictionary<string, object> ret = new Dictionary<string, object>();
                ret.Add("User", md.User);
                ret.Add("Key", md.Key);
                ret.Add("Perm", md.Perm);
                ret.Add("Node", md.Node);
                return new HttpResponse(md.Http, true, 200, null, "application/json", Common.SerializeJson(ret, true), true);
            }
              
            switch (md.Http.Method.ToLower())
            {
                case "get":
                    #region get

                    if (WatsonCommon.UrlEqual(md.Http.RawUrlWithoutQuery, "/token", false))
                    {
                        return HttpGetToken(md);
                    }
                     
                    if (WatsonCommon.UrlEqual(md.Http.RawUrlWithoutQuery, "/user", false))
                    {
                        return HttpGetUser(md);
                    }

                    if (WatsonCommon.UrlEqual(md.Http.RawUrlWithoutQuery, "/containers", false))
                    {
                        return HttpGetContainerList(md);
                    }

                    return HttpGetHandler(md);
                     
                #endregion

                case "put":
                    #region put

                    return HttpPutHandler(md);
                     
                #endregion

                case "post":
                    #region post

                    return HttpPostHandler(md);
                     
                #endregion

                case "delete":
                    #region delete

                    return HttpDeleteHandler(md);
                    
                #endregion

                case "head":
                    #region head

                    return HttpHeadHandler(md);
                     
                #endregion
            }
             
            _Logging.Log(LoggingModule.Severity.Warn, "UserApiHandler unknown URL " + md.Http.RawUrlWithoutQuery);
            return new HttpResponse(md.Http, false, 404, null, "application/json",
                new ErrorResponse(2, 404, "Unknown endpoint.", null), true); 
        }
    }
}