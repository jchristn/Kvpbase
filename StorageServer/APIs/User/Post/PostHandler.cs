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
        public static HttpResponse PostHandler(RequestMetadata md)
        {
            #region Process-by-Operation-Type

            if (Common.IsTrue(Common.IsTrue(md.CurrObj.IsContainer)))
            {
                #region Create-Container

                _Logging.Log(LoggingModule.Severity.Warn, "PostHandler unable to create ContainerPath using POST");
                return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                    new ErrorResponse(2, 400, "Unable to create container path using POST; please use PUT.", null).ToJson(), true);

                #endregion
            }
            else
            {
                #region Create-Object
                
                // the last container may be mistakenly seen as the key
                if (!String.IsNullOrEmpty(md.CurrObj.Key))
                {
                    md.CurrObj.ContainerPath.Add(md.CurrObj.Key);
                    md.CurrObj.Key = null;
                }

                return _Object.Write(md);
                
                #endregion
            }

            #endregion
        }
    }
}