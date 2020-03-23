using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.StorageServer.Classes;

namespace Kvpbase.StorageServer
{
    public partial class Program
    {
        static async Task UserApiHandler(RequestMetadata md)
        {
            string header = _Header + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";

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
                    else if (md.Http.Request.RawUrlEntries.Count == 1)
                    {
                        await HttpGetContainerList(md);
                        return;
                    }
                    else if (md.Http.Request.RawUrlEntries.Count == 2)
                    {
                        await HttpGetContainer(md);
                        return;
                    }
                    else if (md.Http.Request.RawUrlEntries.Count >= 3)
                    {
                        await HttpGetObject(md);
                        return;
                    }
                    break;

                case HttpMethod.PUT:
                    if (md.Http.Request.RawUrlEntries.Count == 2)
                    {
                        await HttpPutContainer(md);
                        return;
                    }
                    else if (md.Http.Request.RawUrlEntries.Count >= 3)
                    {
                        await HttpPutObject(md);
                        return;
                    }
                    break;

                case HttpMethod.POST:
                    if (md.Http.Request.RawUrlEntries.Count == 2)
                    {
                        await HttpPostContainer(md);
                        return;
                    }
                    else if (md.Http.Request.RawUrlEntries.Count >= 3)
                    {
                        await HttpPostObject(md);
                        return;
                    }
                    break;

                case HttpMethod.DELETE:
                    if (md.Http.Request.RawUrlEntries.Count == 2)
                    {
                        await HttpDeleteContainer(md);
                        return;
                    }
                    else if (md.Http.Request.RawUrlEntries.Count >= 3)
                    {
                        await HttpDeleteObject(md);
                        return;
                    }
                    break;

                case HttpMethod.HEAD:
                    if (md.Http.Request.RawUrlEntries.Count == 2)
                    {
                        await HttpHeadContainer(md);
                        return;
                    }
                    else if (md.Http.Request.RawUrlEntries.Count >= 3)
                    {
                        await HttpHeadObject(md);
                        return;
                    }
                    break;
            }
             
            _Logging.Warn(header + "UserApiHandler unknown URL " + md.Http.Request.RawUrlWithoutQuery);
            md.Http.Response.StatusCode = 404;
            md.Http.Response.ContentType = "application/json";
            await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(2, 404, null, null), true));
            return;
        }
    }
}