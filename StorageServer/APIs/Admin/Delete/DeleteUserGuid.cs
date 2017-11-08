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
        public static HttpResponse DeleteUserGuid(RequestMetadata md)
        {
            #region Variables

            Dictionary<string, string> req = new Dictionary<string, string>();
            string userGuid = "";
            string homeDirectory = "";
                
            #endregion

            #region Deserialize

            try
            {
                req = Common.DeserializeJson<Dictionary<string, string>>(md.CurrHttpReq.Data);
                if (req == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "DeleteUserGuid null request after deserialization, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                        true);
                }
            }
            catch (Exception)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteUserGuid unable to deserialize request body");
                return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                    new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                    true);
            }

            #endregion

            #region Set-GUID

            if (req.ContainsKey("user_guid")) userGuid = req["user_guid"];
            if (String.IsNullOrEmpty(userGuid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteUserGuid unable to find value for guid in request body");
                return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                    new ErrorResponse(2, 400, "Unable to validate request body.  Null value supplied for guid.", null).ToJson(),
                    true);
            }

            #endregion

            #region Get-User-Home-Directory

            homeDirectory = _Users.GetHomeDirectory(userGuid, _Settings);
            if (String.IsNullOrEmpty(homeDirectory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteUserGuid unable to find home directory for user GUID " + userGuid);
                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to find home directory for user.", null).ToJson(),
                    true);
            }

            #endregion

            #region Check-Existence

            if (!Common.DirectoryExists(homeDirectory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteUserGuid directory " + homeDirectory + " does not exist");
                return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                    new ErrorResponse(5, 404, "Container does not exist.", null).ToJson(),
                    true);
            }

            #endregion

            #region Process

            if (!Common.DeleteDirectory(homeDirectory, true))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteUserGuid false returned from attempt to delete " + homeDirectory + userGuid);
                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to delete container.", null).ToJson(),
                    true);
            }

            _Logging.Log(LoggingModule.Severity.Debug, "DeleteUserGuid successfully deleted " + homeDirectory);
            return new HttpResponse(md.CurrHttpReq, true, 200, null, "text/plain", null, true);

            #endregion
        }
    }
}