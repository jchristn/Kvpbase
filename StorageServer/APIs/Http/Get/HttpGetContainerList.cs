using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpGetContainerList(RequestMetadata md)
        {
            #region Validate-Authentication

            if (md.User == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpGetContainerList no authentication material");
                return new HttpResponse(md.Http, 401, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
            }

            #endregion

            #region Retrieve-and-Respond

            List<ContainerSettings> containers = new List<ContainerSettings>();
            if (!_ContainerMgr.GetContainers(out containers))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HttpGetContainerList unable to retrieve containers");
                return new HttpResponse(md.Http, 500, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(4, 500, null, null), true)));
            }

            if (containers == null || containers.Count < 1)
            {
                return new HttpResponse(md.Http, 200, null, "application/json", Encoding.UTF8.GetBytes(Common.SerializeJson(new List<object>(), true)));
            }
             
            return new HttpResponse(md.Http, 200, null, "application/json",
                Encoding.UTF8.GetBytes(Common.SerializeJson(containers, true))); 

            #endregion 
        }
    }
}