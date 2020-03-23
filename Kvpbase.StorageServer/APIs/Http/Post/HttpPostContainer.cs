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
        internal static async Task HttpPostContainer(RequestMetadata md)
        {
            string header = _Header + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";
             
            if (md.User == null)
            {
                _Logging.Warn(header + "HttpPostContainer no authentication material");
                md.Http.Response.StatusCode = 401;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                return;
            }
             
            if (_ContainerMgr.Exists(md.User.GUID, md.Params.ContainerName))
            {
                _Logging.Warn(header + "HttpPostContainer container " + md.User.GUID + "/" + md.Params.ContainerName + " already exists");
                md.Http.Response.StatusCode = 409;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(7, 409, null, null), true));
                return;
            }
             
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
                    _Logging.Warn(header + "HttpPostContainer unable to deserialize request body");
                    md.Http.Response.StatusCode = 400;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(9, 400, null, null), true));
                    return;
                }
            }

            if (container == null)
            {
                _Logging.Debug(header + "HttpPostContainer no settings found, using defaults for " + md.User.GUID + "/" + md.Params.ContainerName);
                container = new Container(); 
            }
             
            container.UserGUID = md.User.GUID.ToLower();
            container.Name = md.Params.ContainerName.ToLower();

            if (String.IsNullOrEmpty(container.GUID)) container.GUID = Guid.NewGuid().ToString();
            if (!String.IsNullOrEmpty(md.User.HomeDirectory)) container.ObjectsDirectory = md.User.HomeDirectory + container.Name;
            else container.ObjectsDirectory = _Settings.Storage.Directory + container.UserGUID + "/" + container.Name + "/";
            if (!container.ObjectsDirectory.EndsWith("/")) container.ObjectsDirectory += "/"; 
             
            _ContainerMgr.Add(container); 
            _Logging.Info(header + "HttpPostContainer created container " + container.UserGUID + "/" + container.Name);
            md.Http.Response.StatusCode = 201;
            await md.Http.Response.Send();
            return; 
        } 
    }
}