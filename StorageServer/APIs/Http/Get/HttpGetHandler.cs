using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpGetHandler(RequestMetadata md)
        { 
            bool isContainer = Common.IsTrue(md.Http.RetrieveHeaderValue("_container")); 
            if (isContainer)
            {
                if (md.Http.RawUrlEntries.Count == 1)
                {
                    return HttpGetContainers(md);
                }
                else
                {
                    return HttpGetContainer(md);
                }
            }
            else
            {
                return HttpGetObject(md);
            } 
        }
    }
}