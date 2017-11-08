using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse DeleteHandler(RequestMetadata md)
        {
            #region Process-by-Operation-Type

            bool recursive = Common.IsTrue(md.CurrHttpReq.RetrieveHeaderValue("recursive"));

            if (Common.IsTrue(md.CurrObj.IsContainer))
            {
                #region Delete-Container

                return _Container.Delete(md, recursive);
                
                #endregion
            }
            else
            {
                #region Delete-Object
                
                if (String.IsNullOrEmpty(md.CurrObj.Key))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "DeleteHandler unable to find object key in URL");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to find object key in URL.", null).ToJson(), true);
                }

                return _Object.Delete(md);
                
                #endregion
            }

            #endregion
        }
    }
}