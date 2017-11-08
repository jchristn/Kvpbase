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
                _Logging.Log(LoggingModule.Severity.Info, Common.Line(79, "*"));
                _Logging.Log(LoggingModule.Severity.Info, "");
                _Logging.Log(LoggingModule.Severity.Info, "PostKill requested at " + DateTime.Now + " by " + md.CurrHttpReq.SourceIp + ":" + md.CurrHttpReq.SourcePort);
                _Logging.Log(LoggingModule.Severity.Info, "");
                _Logging.Log(LoggingModule.Severity.Info, Common.Line(79, "*"));

                return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
            }
            finally
            {
                Common.ExitApplication("PostKill", "Kill requested at " + DateTime.Now + " by " + md.CurrHttpReq.SourceIp + ":" + md.CurrHttpReq.SourcePort, -1);
            }
        }
    }
}