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
        public static bool TcpHeadContainer(RequestMetadata md)
        { 
            #region Retrieve-Container
             
            if (!_ContainerHandler.Exists(md, md.Params.UserGuid, md.Params.Container))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpHeadContainer unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                return false;
            }
            else
            {
                return true;
            }
            
            #endregion
        }
    }
}