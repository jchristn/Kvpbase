using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse GetHandler(RequestMetadata md)
        {
            #region Process-by-Operation-Type

            if (Common.IsTrue(md.CurrentObj.IsContainer))
            {
                ContainerHandler ch = new ContainerHandler(CurrentSettings, CurrentTopology, CurrentNode, Users, Maintenance, Logging, Logger);
                return ch.ContainerRead(md);
            }
            else
            {
                ObjectHandler oh = new ObjectHandler(CurrentSettings, CurrentTopology, CurrentNode, Users, LockManager, Maintenance, EncryptionManager, Logging, Logger);
                return oh.ObjectRead(md);
            }

            #endregion
        }
    }
}