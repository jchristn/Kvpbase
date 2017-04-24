using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse GetCleanup(RequestMetadata md)
        {
            Task.Run(() => CleanupThread());
            return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", null, true);
        }

        public static void CleanupThread()
        {
            DateTime startTime = DateTime.Now;

            Logging.Log(LoggingModule.Severity.Info, "CleanupThread starting cleanup at " + startTime);
            Logging.Log(LoggingModule.Severity.Info, "CleanupThread ending cleanup after " + Common.TotalMsFrom(startTime) + "ms");
            return;
        }
    }
}