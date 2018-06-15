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
        public static bool TcpDeleteObject(RequestMetadata md)
        { 
            #region Retrieve-Container
             
            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpDeleteObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                return true;
            }
            
            #endregion
             
            #region Delete-and-Return
             
            if (!_ObjectHandler.Exists(md, currContainer, md.Params.ObjectKey))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpDeleteObject unable to find object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                return true;
            }
            else
            {
                ErrorCode error;
                if (_ObjectHandler.Delete(md, currContainer, md.Params.ObjectKey, out error))
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "TcpDeleteObject deleted object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                }

                return true;
            } 

            #endregion 
        }
    }
}