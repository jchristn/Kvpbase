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
        public static HttpResponse HttpGetDisks(RequestMetadata md)
        {
            List<DiskInfo> ret = DiskInfo.GetAllDisks();
            return new HttpResponse(md.Http, true, 200, null, "application/json",
                Common.SerializeJson(ret, true), true);
        }
    }
}