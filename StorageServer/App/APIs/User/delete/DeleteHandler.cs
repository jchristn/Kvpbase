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

            bool recursive = Common.IsTrue(md.CurrentHttpRequest.RetrieveHeaderValue("recursive"));

            if (Common.IsTrue(md.CurrentObj.IsContainer))
            {
                #region Delete-Container

                ContainerHandler cd = new ContainerHandler(CurrentSettings, CurrentTopology, CurrentNode, Users, Maintenance, Logging, Logger);
                return cd.ContainerDelete(md, recursive);
                
                #endregion
            }
            else
            {
                #region Delete-Object
                
                if (String.IsNullOrEmpty(md.CurrentObj.Key))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "DeleteHandler unable to find object key in URL");
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to find object key in URL.", null).ToJson(), true);
                }

                ObjectHandler oh = new ObjectHandler(CurrentSettings, CurrentTopology, CurrentNode, Users, LockManager, Maintenance, EncryptionManager, Logging, Logger);
                return oh.ObjectDelete(md);
                
                #endregion
            }

            #endregion
        }
    }
}