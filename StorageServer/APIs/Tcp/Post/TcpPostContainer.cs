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
        public static bool TcpPostContainer(RequestMetadata md)
        {   
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
                    _Logging.Log(LoggingModule.Severity.Warn, "TcpPostContainer unable to deserialize request body");
                    return false;
                }
            }

            if (reqBody == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TcpPostContainer invalid request body");
                return false;
            }

            #endregion

            #region Create

            _ContainerHandler.Create(md, reqBody);
            return true;

            #endregion
        }
    }
}