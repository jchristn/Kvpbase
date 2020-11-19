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
        static async Task AdminApiHandler(RequestMetadata md)
        {
            string header = _Header + md.Http.Request.Source.IpAddress + ":" + md.Http.Request.Source.Port + " ";

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
                    if (md.Http.Request.Url.Elements[1].Equals("disk"))
                    {
                        await HttpGetDisks(md);
                        return;
                    }
                    else if (md.Http.Request.Url.Elements[1].Equals("config"))
                    {
                        await HttpGetConfig(md);
                        return;
                    } 
                    break;

                case HttpMethod.PUT: 
                    break;

                case HttpMethod.POST: 
                    break;

                case HttpMethod.DELETE: 
                    break;

                case HttpMethod.HEAD: 
                    break;
            }
            
            _Logging.Warn(header + "AdminApiHandler unknown URL " + md.Http.Request.Url.RawWithoutQuery);
            md.Http.Response.StatusCode = 404;
            md.Http.Response.ContentType = "application/json";
            await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(2, 404, null, null), true));
            return;
        }
    }
}