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
        public static HttpResponse PostKill(RequestMetadata md)
        {
            try
            {
                Logging.Log(LoggingModule.Severity.Info, Common.Line(79, "*"));
                Logging.Log(LoggingModule.Severity.Info, "");
                Logging.Log(LoggingModule.Severity.Info, "PostKill requested at " + DateTime.Now + " by " + md.CurrentHttpRequest.SourceIp + ":" + md.CurrentHttpRequest.SourcePort);
                Logging.Log(LoggingModule.Severity.Info, "");
                Logging.Log(LoggingModule.Severity.Info, Common.Line(79, "*"));

                return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", null, true);
            }
            finally
            {
                Common.ExitApplication("PostKill", "Kill requested at " + DateTime.Now + " by " + md.CurrentHttpRequest.SourceIp + ":" + md.CurrentHttpRequest.SourcePort, -1);
            }
        }
    }
}