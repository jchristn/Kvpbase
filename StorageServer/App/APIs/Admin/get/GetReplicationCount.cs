using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse GetReplicationCount(RequestMetadata md)
        {
            List<string> subdirectories = new List<string>();
            List<string> files = new List<string>();
            long bytes = 0;

            if (Common.WalkDirectory(
                CurrentSettings.Environment,
                0,
                CurrentSettings.Replication.Directory,
                false,
                out subdirectories,
                out files,
                out bytes,
                true))
            {
                int taskCount = 0;
                if (files != null) taskCount = files.Count;

                return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", taskCount.ToString(), true);
            }
            else
            {
                Logging.Log(LoggingModule.Severity.Warn, "GetReplicationCount unable to walk tasks directory, received false response");
                return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                    new ErrorResponse(1, 500, null, null).ToJson(),
                    true);
            }
        }
    }
}