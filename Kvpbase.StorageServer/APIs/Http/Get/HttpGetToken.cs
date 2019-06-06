using System;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Core;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpGetToken(RequestMetadata md)
        {
            if (md == null || md.User == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpGetToken no metadata supplied");
                return new HttpResponse(md.Http, 401, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unable to authenticate.", null), true)));
            }
            else
            {
                return new HttpResponse(md.Http, 200, null, "text/plain", Encoding.UTF8.GetBytes(_TokenMgr.TokenFromUser(md.User)));
            }
        }
    }
}