using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Classes;

namespace Kvpbase
{
    public partial class StorageServer
    {
        static async Task UserApiHandler(RequestMetadata md)
        {
            string header = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";
             
            if (md.Params.RequestMetadata)
            {
                md.Http.Response.StatusCode = 200;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(md, true));
                return;
            }
              
            switch (md.Http.Request.Method)
            {
                case HttpMethod.GET: 
                    if (md.Http.Request.RawUrlWithoutQuery.Equals("/containers"))
                    {
                        await HttpGetContainerList(md);
                        return;
                    }
                      
                    await HttpGetHandler(md);
                    return;

                case HttpMethod.PUT: 
                    await HttpPutHandler(md);
                    return;

                case HttpMethod.POST: 
                    await HttpPostHandler(md);
                    return;

                case HttpMethod.DELETE: 
                    await HttpDeleteHandler(md);
                    return;

                case HttpMethod.HEAD: 
                    await HttpHeadHandler(md);
                    return;
            }
             
            _Logging.Warn(header + "UserApiHandler unknown URL " + md.Http.Request.RawUrlWithoutQuery);
            md.Http.Response.StatusCode = 404;
            md.Http.Response.ContentType = "application/json";
            await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(2, 404, null, null), true));
            return;
        }
    }
}