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
        internal static async Task HttpGetDisks(RequestMetadata md)
        {
            List<DiskInfo> ret = DiskInfo.GetAllDisks();
            md.Http.Response.StatusCode = 200;
            md.Http.Response.ContentType = "application/json";
            await md.Http.Response.Send(Common.SerializeJson(ret, true));
            return;
        }
    }
}