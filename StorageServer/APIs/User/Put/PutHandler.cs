using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse PutHandler(RequestMetadata md)
        {
            if (Common.IsTrue(md.CurrObj.IsContainer))
            {
                return _Container.Write(md);
            }
            else
            {
                if (String.IsNullOrEmpty(md.CurrObj.Key))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "PutHandler unable to find key in URL");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to find object key in URL.", null).ToJson(), true);
                }

                return _Object.Write(md);
            }
        }
    }
}