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
        public static bool TcpPostObject(RequestMetadata md)
        { 
            #region Retrieve-Container
                 
            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                _Logging.Warn("TcpPostObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                return false;
            }

            #endregion

            #region Check-if-Object-Exists

            if (currContainer.Exists(md.Params.ObjectKey))
            {
                _Logging.Warn("TcpPostObject object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " already exists");
                return false;
            }

            #endregion

            #region Write-and-Return

            ErrorCode error;
            if (!_ObjectHandler.Create(md, currContainer, md.Params.ObjectKey, md.Http.ContentType, md.Http.ContentLength, md.Http.DataStream, out error))
            {
                _Logging.Warn("TcpPostObject unable to write object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + ": " + error.ToString());
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