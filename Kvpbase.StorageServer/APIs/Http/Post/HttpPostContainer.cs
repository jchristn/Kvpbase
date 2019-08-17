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
        public static HttpResponse HttpPostContainer(RequestMetadata md)
        {
            bool cleanupRequired = false; 
            ContainerSettings settings = null;

            try
            {
                #region Validate-Authentication

                if (md.User == null)
                {
                    _Logging.Warn("HttpPostContainer no authentication material");
                    return new HttpResponse(md.Http, 401, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                }

                #endregion
                 
                #region Check-if-Container-Exists
                 
                if (_ContainerMgr.Exists(md.User.Guid, md.Params.Container))
                {
                    _Logging.Warn("HttpPostContainer container " + md.User.Guid + "/" + md.Params.Container + " already exists");
                    return new HttpResponse(md.Http, 409, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(7, 409, null, null), true)));
                }

                #endregion

                #region Deserialize-Request-Body

                if (md.Http.DataStream != null && md.Http.DataStream.CanRead)
                {
                    md.Http.Data = Common.StreamToBytes(md.Http.DataStream);

                    try
                    {
                        settings = Common.DeserializeJson<ContainerSettings>(md.Http.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Warn("HttpPostContainer unable to deserialize request body");
                        return new HttpResponse(md.Http, 400, null, "application/json",
                            Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(9, 400, null, null), true)));
                    }
                }

                if (settings == null)
                {
                    _Logging.Debug("HttpPostContainer no settings found, using defaults for " + md.User.Guid + "/" + md.Params.Container);
                    settings = new ContainerSettings(); 
                }

                #endregion

                #region Apply-Base-Settings

                settings.User = md.User.Guid.ToLower();
                settings.Name = md.Params.Container.ToLower();

                if (!String.IsNullOrEmpty(md.User.HomeDirectory)) settings.RootDirectory = md.User.HomeDirectory + settings.Name;
                else settings.RootDirectory = _Settings.Storage.Directory + settings.User + "/" + settings.Name + "/";

                settings.DatabaseFilename = settings.RootDirectory + "__Container__.db";
                settings.ObjectsDirectory = settings.RootDirectory + "__Objects__/";
                settings.HandlerType = ObjectHandlerType.Disk; 

                #endregion

                #region Create-and-Replicate

                _ContainerHandler.Create(md, settings);
                md.Http.Data = Encoding.UTF8.GetBytes(Common.SerializeJson(settings, false));

                if (!_OutboundMessageHandler.ContainerCreate(md, settings))
                {
                    _Logging.Warn("HttpPostContainer unable to replicate operation to one or more nodes");
                    cleanupRequired = true;

                    return new HttpResponse(md.Http, 500, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(10, 500, null, null), true)));
                }

                _Logging.Debug("HttpPostContainer successfully created container " + settings.User + "/" + settings.Name);
                return new HttpResponse(md.Http, 201, null);

                #endregion
            }
            finally
            {
                if (cleanupRequired)
                {
                    _ContainerHandler.Delete(settings.User, settings.Name);
                    _OutboundMessageHandler.ContainerDelete(md, settings);
                }
            }
        } 
    }
}