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
        public static HttpResponse HttpDeleteHandler(RequestMetadata md)
        {
            bool isContainer = Common.IsTrue(md.Http.RetrieveHeaderValue("_container"));
            if (isContainer)
            {
                if (md.Http.RawUrlEntries.Count == 2)
                {
                    return HttpDeleteContainer(md);
                }
                else
                {
                    _Logging.Warn("HttpDeleteHandler container URL does not have two entries");
                    return new HttpResponse(md.Http, 400, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(2, 400, "URL path must contain two entries, i.e. /[user]/[container]/.", null), true)));
                }
            }
            else
            {
                if (md.Http.RawUrlEntries.Count == 3)
                {
                    return HttpDeleteObject(md);
                }
                else
                {
                    _Logging.Warn("HttpDeleteHandler object URL does not have three entries");
                    return new HttpResponse(md.Http, 400, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(2, 400, "URL path must contain three entries, i.e. /[user]/[container]/[key].", null), true)));
                }
            }
        }
    }
}