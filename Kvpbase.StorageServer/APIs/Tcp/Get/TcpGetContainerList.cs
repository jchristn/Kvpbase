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
        public static bool TcpGetContainerList(RequestMetadata md, out List<ContainerSettings> containers)
        {
            containers = new List<ContainerSettings>();

            #region Retrieve-and-Respond
             
            if (!_ContainerMgr.GetContainers(out containers))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpGetContainerList unable to retrieve containers");
                return false;
            }
             
            return true;

            #endregion
        }
    }
}