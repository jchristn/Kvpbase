using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Container;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static bool TcpGetObject(RequestMetadata md, out ObjectMetadata metadata, out byte[] data)
        {
            metadata = null;
            data = null;

            #region Retrieve-Container

            Container.Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpGetObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                return false;
            }

            #endregion

            #region Retrieve-Metadata

            if (!currContainer.ReadObjectMetadata(md.Params.ObjectKey, out metadata))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpGetObject unable to retrieve metadata for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
                return false;
            }

            if (md.Params.Metadata) return true;

            #endregion

            #region Retrieve-and-Return
             
            int? index = null;
            if (md.Params.Index != null) index = Convert.ToInt32(md.Params.Index);

            int? count = null;
            if (md.Params.Count != null) count = Convert.ToInt32(md.Params.Count);

            string contentType = null;
            ErrorCode error;

            if (!_ObjectHandler.Read(md, currContainer, md.Params.ObjectKey, index, count, out contentType, out data, out error))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpGetObject unable to find object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey);
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