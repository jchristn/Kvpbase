using System;
using System.Collections.Generic;
using System.Linq;
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
        public static bool TcpGetContainer(RequestMetadata md, out ContainerMetadata meta)
        {
            meta = null;

            #region Retrieve-Container

            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpGetContainer unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                return false;
            }
             
            #endregion
             
            #region Enumerate-and-Return

            int? index = null;
            if (md.Params.Index != null) index = Convert.ToInt32(md.Params.Index);

            int? count = null;
            if (md.Params.Count != null) count = Convert.ToInt32(md.Params.Count);
              
            meta = _ContainerHandler.Enumerate(md, currContainer, index, count, md.Params.OrderBy);
             
            return true;

            #endregion 
        }
    }
}