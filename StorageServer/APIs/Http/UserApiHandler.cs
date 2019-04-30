using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        static HttpResponse UserApiHandler(RequestMetadata md)
        {  
            if (md.Params.RequestMetadata)
            {
                RequestMetadata respMetadata = md.Sanitized();
                return new HttpResponse(md.Http, 200, null, "application/json", Encoding.UTF8.GetBytes(Common.SerializeJson(respMetadata, true)));
            }
              
            switch (md.Http.Method)
            {
                case HttpMethod.GET:
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

                case HttpMethod.PUT:
                    #region put

                    return HttpPutHandler(md);
                     
                #endregion

                case HttpMethod.POST:
                    #region post

                    return HttpPostHandler(md);
                     
                #endregion

                case HttpMethod.DELETE:
                    #region delete

                    return HttpDeleteHandler(md);
                    
                #endregion

                case HttpMethod.HEAD:
                    #region head

                    return HttpHeadHandler(md);
                     
                #endregion
            }
             
            _Logging.Log(LoggingModule.Severity.Warn, "UserApiHandler unknown URL " + md.Http.RawUrlWithoutQuery);
            return new HttpResponse(md.Http, 404, null, "application/json",
                Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(2, 404, "Unknown endpoint.", null), true))); 
        }
    }
}