using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HeadHandler(RequestMetadata md)
        {
            #region Process-by-Operation-Type

            if (Common.IsTrue(md.CurrObj.IsContainer))
            {
                #region Get-Container

                return _Container.Head(md);

                #endregion
            }
            else
            {
                #region Get-Object

                return _Object.Head(md);

                #endregion
            }

            #endregion
        }
    }
}