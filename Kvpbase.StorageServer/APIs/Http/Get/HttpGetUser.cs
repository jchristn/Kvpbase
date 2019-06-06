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
        public static HttpResponse HttpGetUser(RequestMetadata md)
        {
            if (md == null || md.User == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpGetUser no metadata supplied");
                return new HttpResponse(md.Http, 401, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unable to authenticate.", null), true)));
            }
            else
            {
                md.User.Password = null;
                return new HttpResponse(md.Http, 200, null, "text/plain", Encoding.UTF8.GetBytes(Common.SerializeJson(md.User, true)));
            }
        }
    }
}