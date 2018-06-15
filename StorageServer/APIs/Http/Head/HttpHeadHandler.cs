using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpHeadHandler(RequestMetadata md)
        {
            bool isContainer = Common.IsTrue(md.Http.RetrieveHeaderValue("_container"));
            if (isContainer)
            {
                return HttpHeadContainer(md);
            }
            else
            {
                return HttpHeadObject(md);
            }
        }
    }
}