using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpGetUser(RequestMetadata md)
        {
            if (md == null || md.User == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpGetUser no metadata supplied");
                return new HttpResponse(md.Http, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Unable to authenticate.", null), true);
            }
            else
            {
                md.User.Password = null;
                return new HttpResponse(md.Http, true, 200, null, "text/plain", Common.SerializeJson(md.User, true), true);
            }
        }
    }
}