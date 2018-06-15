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
        public static HttpResponse HttpGetTopology(RequestMetadata md)
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("Nodes", _TopologyMgr.GetNodes());
            ret.Add("Replicas", _TopologyMgr.GetReplicas());
            return new HttpResponse(md.Http, true, 200, null, "application/json",
                Common.SerializeJson(ret, true), true);
        }
    }
}