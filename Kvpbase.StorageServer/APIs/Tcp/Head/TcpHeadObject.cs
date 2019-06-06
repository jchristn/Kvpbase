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
        public static bool TcpHeadObject(RequestMetadata md)
        { 
            #region Retrieve-Container
             
            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpHeadObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                return false;
            }

            bool isPublicRead = currContainer.IsPublicRead();

            #endregion
             
            #region Retrieve-and-Return
             
            if (!_ObjectHandler.Exists(md, currContainer, md.Params.ObjectKey))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpHeadObject unable to find object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
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