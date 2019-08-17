using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpPutContainer(RequestMetadata md)
        {
            #region Validate-Authentication

            if (md.User == null)
            {
                _Logging.Warn("HttpPutContainer no authentication material");
                return new HttpResponse(md.Http, 401, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
            }

            #endregion

            #region Validate-Request

            if (md.Http.RawUrlEntries.Count != 2)
            {
                _Logging.Warn("HttpPutContainer request URL does not have two entries");
                return new HttpResponse(md.Http, 400, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(2, 400, "URL path must contain two entries, i.e. /[user]/[container]/.", null), true)));
            }
             
            if (!md.Params.UserGuid.ToLower().Equals(md.User.Guid.ToLower()))
            {
                _Logging.Warn("HttpPutContainer user " + md.User.Guid + " attempting to PUT container in user " + md.Params.UserGuid);
                return new HttpResponse(md.Http, 401, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
            }

            #endregion

            #region Check-if-Container-Exists

            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                List<Node> nodes = new List<Node>();
                if (!_OutboundMessageHandler.FindContainerOwners(md, out nodes))
                {
                    _Logging.Warn("HttpPutContainer unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, 404, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(5, 404, "Unknown user or container.", null), true)));
                }
                else
                {
                    string redirectUrl = null;
                    HttpResponse redirectRest = _OutboundMessageHandler.BuildRedirectResponse(md, nodes[0], out redirectUrl);
                    _Logging.Debug("HttpPutContainer redirecting container " + md.Params.UserGuid + "/" + md.Params.Container + " to " + redirectUrl);
                    return redirectRest;
                }
            }

            #endregion

            #region Process

            if (md.Params.AuditLog)
            {
                #region Audit-Log

                int count = 100;
                int index = 0;
                if (md.Params.Count != null) count = Convert.ToInt32(md.Params.Count);
                if (md.Params.Index != null) index = Convert.ToInt32(md.Params.Index);

                List<AuditLogEntry> entries = currContainer.GetAuditLogEntries(
                    md.Params.AuditKey,
                    md.Params.Action,
                    count,
                    index,
                    md.Params.CreatedBefore,
                    md.Params.CreatedAfter);

                return new HttpResponse(md.Http, 200, null, "application/json", Encoding.UTF8.GetBytes(Common.SerializeJson(entries, true)));

                #endregion
            }
            else
            {
                #region Update

                #region Deserialize-Request-Body

                ContainerSettings settings = null;

                if (md.Http.DataStream != null && md.Http.DataStream.CanRead)
                {
                    md.Http.Data = Common.StreamToBytes(md.Http.DataStream);

                    try
                    {
                        settings = Common.DeserializeJson<ContainerSettings>(md.Http.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Warn("HttpPutContainer unable to deserialize request body");
                        return new HttpResponse(md.Http, 400, null, "application/json",
                            Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(9, 400, null, null), true)));
                    }
                }

                if (settings == null)
                {
                    _Logging.Warn("HttpPutContainer no request body");
                    return new HttpResponse(md.Http, 400, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(2, 400, "No container settings found in request body.", null), true)));
                }

                #endregion

                #region Update

                _ContainerHandler.Update(md, currContainer, settings);

                _OutboundMessageHandler.ContainerUpdate(md, settings);

                return new HttpResponse(md.Http, 200, null);

                #endregion

                #endregion
            }

            #endregion
        }
    }
}