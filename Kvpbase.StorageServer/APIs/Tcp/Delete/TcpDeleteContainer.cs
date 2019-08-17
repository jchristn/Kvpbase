using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static bool TcpDeleteContainer(RequestMetadata md)
        {
            #region Retrieve-Container

            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                _Logging.Warn("TcpDeleteContainer unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                return true;
            }

            if (md.Params.AuditLog)
            {
                currContainer.ClearAuditLog(); 
                _Logging.Info("TcpDeleteContainer cleared audit log for container " + md.Params.UserGuid + "/" + md.Params.Container);
                return true;
            }
            else
            {
                _ContainerHandler.Delete(md.Params.UserGuid, md.Params.Container);
                _Logging.Info("TcpDeleteContainer deleted container " + md.Params.UserGuid + "/" + md.Params.Container);
                return true;
            }
            
            #endregion
        }
    }
}