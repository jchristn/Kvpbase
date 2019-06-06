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
        public static bool TcpPutContainer(RequestMetadata md)
        {  
            #region Check-if-Container-Exists

            Container currContainer;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpPutContainer container " + md.Params.UserGuid + "/" + md.Params.Container + " not found");
                return false;
            }

            #endregion

            #region Deserialize-Request-Body

            ContainerSettings reqBody = null;
            if (md.Http.Data != null && md.Http.Data.Length > 0)
            {
                try
                {
                    reqBody = Common.DeserializeJson<ContainerSettings>(md.Http.Data);
                }
                catch (Exception)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "TcpPutContainer unable to deserialize request body");
                    return false;
                }
            }

            if (reqBody == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpPutContainer no request body");
                return false;
            }

            #endregion

            #region Update

            _ContainerHandler.Update(md, currContainer, reqBody);
            return true;

            #endregion
        }
    }
}