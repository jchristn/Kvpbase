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
            curr.NodeId = Convert.ToInt32(_Node.NodeId);
            curr.Name = _Node.Name;
            curr.DnsHostname = _Node.DnsHostname;
            curr.Port = _Node.Port;
            curr.Ssl = _Node.Ssl;

            return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(curr), true);
        }
    }
}