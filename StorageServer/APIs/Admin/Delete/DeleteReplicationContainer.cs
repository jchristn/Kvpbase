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
        public static HttpResponse DeleteReplicationContainer(RequestMetadata md)
        {
            #region Deserialize

            Obj req = new Obj();
            try
            {
                req = Common.DeserializeJson<Obj>(md.CurrHttpReq.Data);
                if (req == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "DeleteReplicationContainer null request after deserialization, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                }
            }
            catch (Exception)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteReplicationContainer unable to deserialize request body");
                return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                    new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
            }

            #endregion

            #region Build-Disk-Path

            req.DiskPath = Obj.BuildDiskPath(req, md.CurrUser, _Settings, _Logging);
            if (String.IsNullOrEmpty(req.DiskPath))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteReplicationContainer unable to build disk path from request");
                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
            }

            #endregion

            #region Process

            if (Common.DirectoryExists(req.DiskPath))
            {
                if (Common.DeleteDirectory(req.DiskPath, true))
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "DeleteReplicationContainer successfully deleted " + req.DiskPath);
                    return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                }

                _Logging.Log(LoggingModule.Severity.Warn, "DeleteReplicationContainer unable to delete " + req.DiskPath);
                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to delete container.", null).ToJson(), true);
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteReplicationContainer unable to find " + req.DiskPath);
                return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                    new ErrorResponse(5, 404, "Container does not exist.", null).ToJson(), true);
            }

            #endregion
        }
    }
}