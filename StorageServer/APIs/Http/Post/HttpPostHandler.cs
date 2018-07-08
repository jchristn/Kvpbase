using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpPostHandler(RequestMetadata md)
        {
            bool isContainer = Common.IsTrue(md.Http.RetrieveHeaderValue("_container"));
            if (isContainer)
            {
                if (md.Http.RawUrlEntries.Count == 2)
                {
                    return HttpPostContainer(md);
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostHandler container URL does not have two entries");
                    return new HttpResponse(md.Http, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "URL path must contain two entries, i.e. /[user]/[container]/.", null), true);
                }
            }
            else
            {
                if (md.Http.RawUrlEntries.Count == 3)
                {
                    return HttpPostObject(md);
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HttpPostHandler object URL does not have three entries");
                    return new HttpResponse(md.Http, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "URL path must contain three entries, i.e. /[user]/[container]/[key].", null), true);
                }
            }
        }
    }
}