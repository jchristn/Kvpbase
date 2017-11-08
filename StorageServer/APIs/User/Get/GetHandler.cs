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

            if (Common.IsTrue(md.CurrObj.IsContainer))
            {
                return _Container.Read(md);
            }
            else
            {
                return _Object.Read(md);
            }

            #endregion
        }
    }
}