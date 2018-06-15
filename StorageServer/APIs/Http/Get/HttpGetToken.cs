using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpGetToken(RequestMetadata md)
        {
            if (md == null || md.User == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpGetToken no metadata supplied");
                return new HttpResponse(md.Http, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Unable to authenticate.", null), true);
            }
            else
            {
                return new HttpResponse(md.Http, true, 200, null, "text/plain", _TokenMgr.TokenFromUser(md.User), true);
            }
        }
    }
}