using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse GetHeartbeat(RequestMetadata md)
        {
            Heartbeat curr = new Heartbeat();
            curr.NodeId = Convert.ToInt32(CurrentNode.NodeId);
            curr.Name = CurrentNode.Name;
            curr.DnsHostname = CurrentNode.DnsHostname;
            curr.Port = CurrentNode.Port;
            curr.Ssl = CurrentNode.Ssl;

            return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", Common.SerializeJson(curr), true);
        }
    }
}