using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Core;

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

                    if (md.Http.RawUrlWithoutQuery.Equals("/containers"))
                    {
                        return HttpGetContainerList(md);
                    }

                    if (md.Http.RawUrlWithoutQuery.Equals("/token"))
                    {
                        return HttpGetToken(md);
                    }
                     
                    if (md.Http.RawUrlWithoutQuery.Equals("/user"))
                    {
                        return HttpGetUser(md);
                    }

                    if (md.Http.RawUrlWithoutQuery.Equals("/version"))
                    {
                        return new HttpResponse(md.Http, 200, null, "text/plain", Encoding.UTF8.GetBytes(_Version));
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
             
            _Logging.Warn("UserApiHandler unknown URL " + md.Http.RawUrlWithoutQuery);
            return new HttpResponse(md.Http, 404, null, "application/json",
                Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(2, 404, "Unknown endpoint.", null), true))); 
        }
    }
}