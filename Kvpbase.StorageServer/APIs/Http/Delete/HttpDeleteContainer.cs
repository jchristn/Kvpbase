using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Containers;
using Kvpbase.Classes;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static async Task HttpDeleteContainer(RequestMetadata md)
        {
            string header = md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";

            #region Validate-Authentication

            if (md.User == null)
            {
                _Logging.Warn(header + "HttpDeleteContainer no authentication material");
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }

            #endregion

            #region Validate-Request

            if (!md.Params.UserGuid.ToLower().Equals(md.User.GUID.ToLower()))
            {
                _Logging.Warn(header + "HttpDeleteContainer user " + md.User.GUID + " attempting to create container in user " + md.Params.UserGuid);
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }

            if (!md.Perm.DeleteContainer)
            {
                _Logging.Warn(header + "HttpDeleteContainer unauthorized delete attempt to container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }

            #endregion

            #region Retrieve-Container-Client

            ContainerClient client = null;
            if (!_ContainerMgr.GetContainerClient(md.Params.UserGuid, md.Params.ContainerName, out client))
            {
                _Logging.Warn(header + "HttpDeleteContainer container " + md.Params.UserGuid + "/" + md.Params.ContainerName + " does not exist");
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return; 
            }

            #endregion

            #region Process

            if (md.Params.AuditLog)
            {
                client.ClearAuditLog(); 
                _Logging.Info(header + "HttpDeleteContainer cleared audit log for container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 204;
                await md.Http.Response.Send();
                return; 
            }
            else if (md.Params.Keys)
            {
                client.WriteContainerKeyValuePairs(null);
                md.Http.Response.StatusCode = 204;
                await md.Http.Response.Send();
                return;
            }
            else
            {
                _ContainerMgr.Delete(md.Params.UserGuid, md.Params.ContainerName, true);
                _Logging.Info(header + "HttpDeleteContainer deleted container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 204;
                await md.Http.Response.Send();
                return;
            }

            #endregion
        }
    }
}