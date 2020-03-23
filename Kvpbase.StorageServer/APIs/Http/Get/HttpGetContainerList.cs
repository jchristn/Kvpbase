using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;
using Kvpbase.StorageServer.Classes; 
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer
{
    public partial class Program
    {
        internal static async Task HttpGetContainerList(RequestMetadata md)
        {
            string header = _Header + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";
             
            if (md.User == null)
            {
                _Logging.Warn(header + "HttpGetContainerList no authentication material");
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }

            if (!md.Http.Request.RawUrlEntries[0].Equals(md.User.GUID))
            {
                _Logging.Warn(header + "HttpGetContainerList attempt to list containers for another user");
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }
             
            List<Container> containers = _ContainerMgr.GetContainersByUser(md.User.GUID); 
            if (containers == null || containers.Count < 1)
            {
                _Logging.Warn(header + "HttpGetContainerList no containers found for user " + md.Http.Request.RawUrlEntries[0]);
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(2, 404, null, null), true));
                return;
            }

            List<string> ret = new List<string>();
            foreach (Container curr in containers) ret.Add(curr.Name);

            md.Http.Response.StatusCode = 200;
            md.Http.Response.ContentType = "application/json";
            await md.Http.Response.Send(Common.SerializeJson(ret, true));
            return; 
        }
    }
}