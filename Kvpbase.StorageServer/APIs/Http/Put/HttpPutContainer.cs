using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;
using Kvpbase.StorageServer.Classes;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer
{
    public partial class Program
    {
        internal static async Task HttpPutContainer(RequestMetadata md)
        {
            string header = _Header + md.Http.Request.Source.IpAddress + ":" + md.Http.Request.Source.Port + " ";
             
            if (md.User == null)
            {
                _Logging.Warn(header + "HttpPutContainer no authentication material");
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }
             
            if (md.Http.Request.Url.Elements.Length != 2)
            {
                _Logging.Warn(header + "HttpPutContainer request URL does not have two entries");
                md.Http.Response.StatusCode = 400;
                md.Http.Response.ContentType = "application.json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(2, 400, "URL path must be of the form /[user]/[container]/[key].", null), true));
                return;
            }
             
            if (!md.Params.UserGUID.ToLower().Equals(md.User.GUID.ToLower()))
            {
                _Logging.Warn(header + "HttpPutContainer user " + md.User.GUID + " attempting to PUT container in user " + md.Params.UserGUID);
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }

            ContainerClient client = _ContainerMgr.GetContainerClient(md.Params.UserGUID, md.Params.ContainerName);
            if (client == null)
            { 
                _Logging.Warn(header + "HttpPutContainer unable to find container " + md.Params.UserGUID + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }
             
            if (md.Params.AuditLog)
            {
                #region Audit-Log

                AuditLogEntryType? entryType = null;
                if (!String.IsNullOrEmpty(md.Params.Action))
                {
                    try
                    {
                        entryType = (AuditLogEntryType)(Enum.Parse(typeof(AuditLogEntryType), md.Params.Action));
                    }
                    catch (Exception)
                    {
                        _Logging.Warn(header + "HttpPutContainer invalid action: " + md.Params.Action);
                        md.Http.Response.StatusCode = 400;
                        md.Http.Response.ContentType = "application.json";
                        await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(2, 400, "Invalid value for 'action' in querystring.", null), true));
                        return;
                    }
                }
                
                int count = 100;
                int index = 0;
                if (md.Params.Count != null) count = Convert.ToInt32(md.Params.Count);
                if (md.Params.Index != null) index = Convert.ToInt32(md.Params.Index);

                List<AuditLogEntry> entries = client.GetAuditLogEntries(
                    md.Params.AuditKey,
                    entryType,
                    count,
                    index,
                    md.Params.CreatedBefore,
                    md.Params.CreatedAfter);

                md.Http.Response.StatusCode = 200;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(entries, true));
                return;

                #endregion
            }
            else if (md.Params.Keys)
            {
                #region Update-Keys

                Dictionary<string, string> keys = null;

                if (md.Http.Request.Data != null && md.Http.Request.ContentLength > 0)
                {
                    byte[] reqData = Common.StreamToBytes(md.Http.Request.Data);

                    try
                    {
                        keys = Common.DeserializeJson<Dictionary<string, string>>(reqData);
                    }
                    catch (Exception)
                    {
                        _Logging.Warn(header + "HttpPutContainer unable to deserialize request body");
                        md.Http.Response.StatusCode = 400;
                        md.Http.Response.ContentType = "application/json";
                        await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(9, 400, null, null), true));
                        return;
                    }
                }

                client.WriteContainerKeyValuePairs(keys);
                md.Http.Response.StatusCode = 201;
                await md.Http.Response.Send();
                return;

                #endregion
            }
            else if (md.Params.Search)
            {
                #region Search

                #region Deserialize-Request-Body

                EnumerationFilter filter = null;

                if (md.Http.Request.Data != null && md.Http.Request.ContentLength > 0)
                {
                    byte[] reqData = Common.StreamToBytes(md.Http.Request.Data);

                    try
                    {
                        filter = Common.DeserializeJson<EnumerationFilter>(reqData);
                    }
                    catch (Exception)
                    {
                        _Logging.Warn(header + "HttpPutContainer unable to deserialize request body");
                        md.Http.Response.StatusCode = 400;
                        md.Http.Response.ContentType = "application/json";
                        await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(9, 400, null, null), true));
                        return;
                    }
                }

                #endregion
                 
                #region Enumerate-and-Return

                int? index = null;
                if (md.Params.Index != null) index = Convert.ToInt32(md.Params.Index);

                int? count = null;
                if (md.Params.Count != null) count = Convert.ToInt32(md.Params.Count);

                ContainerMetadata meta = client.Enumerate(
                    (int?)md.Params.Index,
                    (int?)md.Params.Count,
                    filter,
                    md.Params.OrderBy);

                if (md.Params.Html)
                {
                    md.Http.Response.StatusCode = 200;
                    md.Http.Response.ContentType = "text/html";
                    await md.Http.Response.Send(DirectoryListingPage(meta));
                    return;
                }
                else
                {
                    md.Http.Response.StatusCode = 200;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(meta, true));
                    return;
                }

                #endregion

                #endregion
            }
            else
            {
                #region Update

                #region Deserialize-Request-Body

                Container container = null;

                if (md.Http.Request.Data != null && md.Http.Request.ContentLength > 0)
                {
                    byte[] reqData = Common.StreamToBytes(md.Http.Request.Data);

                    try
                    {
                        container = Common.DeserializeJson<Container>(reqData);
                    }
                    catch (Exception)
                    {
                        _Logging.Warn(header + "HttpPutContainer unable to deserialize request body");
                        md.Http.Response.StatusCode = 400;
                        md.Http.Response.ContentType = "application/json";
                        await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(9, 400, null, null), true));
                        return;
                    }
                }

                if (container == null)
                {
                    _Logging.Warn(header + "HttpPutContainer no request body");
                    md.Http.Response.StatusCode = 400;
                    md.Http.Response.ContentType = "application.json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(2, 400, "No container settings found in request body.", null), true));
                    return;
                }

                #endregion

                #region Update

                _ContainerMgr.Update(container);

                md.Http.Response.StatusCode = 200;
                await md.Http.Response.Send();
                return;

                #endregion

                #endregion
            } 
        }
    }
}