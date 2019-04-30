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
        public static HttpResponse HttpGetDisks(RequestMetadata md)
        {
            List<DiskInfo> ret = DiskInfo.GetAllDisks();
            return new HttpResponse(md.Http, 200, null, "application/json",
                Encoding.UTF8.GetBytes(Common.SerializeJson(ret, true)));
        }
    }
}