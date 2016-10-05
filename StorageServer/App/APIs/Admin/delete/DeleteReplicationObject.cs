﻿using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse DeleteReplicationObject(RequestMetadata md)
        {
            #region Deserialize

            Obj req = new Obj();
            try
            {
                req = Common.DeserializeJson<Obj>(md.CurrentHttpRequest.Data);
                if (req == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "DeleteReplicationObject null request after deserialization, returning 400");
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                        true);
                }
            }
            catch (Exception)
            {
                Logging.Log(LoggingModule.Severity.Warn, "DeleteReplicationObject unable to deserialize request body");
                return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                    new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                    true);
            }

            #endregion

            #region Build-Disk-Path

            req.DiskPath = Obj.BuildDiskPath(req, md.CurrentUserMaster, CurrentSettings, Logging);
            if (String.IsNullOrEmpty(req.DiskPath))
            {
                Logging.Log(LoggingModule.Severity.Warn, "DeleteReplicationObject unable to build disk path from request");
                return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(),
                    true);
            }

            #endregion

            #region Process

            if (Common.FileExists(req.DiskPath))
            {
                if (Common.DeleteFile(req.DiskPath))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "DeleteReplicationObject successfully deleted " + req.DiskPath);
                    return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "text/plain", null, true);
                }

                Logging.Log(LoggingModule.Severity.Warn, "DeleteReplicationObject unable to delete " + req.DiskPath);
                return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to delete object.", null).ToJson(),
                    true);
            }
            else
            {
                Logging.Log(LoggingModule.Severity.Warn, "DeleteReplicationObject unable to find " + req.DiskPath);
                return new HttpResponse(md.CurrentHttpRequest, false, 404, null, "application/json",
                    new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(),
                    true);
            }

            #endregion
        }
    }
}