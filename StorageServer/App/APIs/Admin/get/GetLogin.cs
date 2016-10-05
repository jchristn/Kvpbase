using System;
using System.Collections.Generic;
using System.Data;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse GetLogin(RequestMetadata md)
        {
            #region Variables

            string email = "";
            string password = "";
            UserMaster currUser = new UserMaster();

            #endregion

            #region Get-Values-from-Querystring
                
            email = md.CurrentHttpRequest.RetrieveHeaderValue(CurrentSettings.Server.HeaderEmail);
            password = md.CurrentHttpRequest.RetrieveHeaderValue(CurrentSettings.Server.HeaderPassword);
                
            if (String.IsNullOrEmpty(email))
            {
                Logging.Log(LoggingModule.Severity.Warn, "GetLogin email not found in querystring under key " + CurrentSettings.Server.HeaderEmail);
                return new HttpResponse(md.CurrentHttpRequest, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Incomplete authentication material supplied.", null).ToJson(),
                    true);
            }

            if (String.IsNullOrEmpty(password))
            {
                Logging.Log(LoggingModule.Severity.Warn, "GetLogin password not found in querystring under key " + CurrentSettings.Server.HeaderPassword);
                return new HttpResponse(md.CurrentHttpRequest, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Incomplete authentication material supplied.", null).ToJson(),
                    true);
            }

            #endregion

            #region Retrieve-User-Master-by-Email

            currUser = Users.GetUserByEmail(email);
            if (currUser == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "GetLogin unable to find user while attempting to authenticate user " + email);
                return new HttpResponse(md.CurrentHttpRequest, false, 401, null, "application/json", null, true);
            }

            #endregion

            #region Check-Password

            if (String.Compare(password, currUser.Password) != 0)
            {
                Logging.Log(LoggingModule.Severity.Warn, "GetLogin incorrect password supplied for user " + email);
                return new HttpResponse(md.CurrentHttpRequest, false, 401, null, "application/json", null, true);
            }

            #endregion

            #region Check-User-Active

            if (!Common.IsTrue(currUser.Active))
            {
                Logging.Log(LoggingModule.Severity.Warn, "GetLogin user " + email + " marked inactive");
                return new HttpResponse(md.CurrentHttpRequest, false, 401, null, "application/json", null, true);
            }

            #endregion

            #region Check-User-Not-Expired

            if (!Common.IsLaterThanNow(currUser.Expiration))
            {
                Logging.Log(LoggingModule.Severity.Warn, "GetLogin user " + email + " marked as expired at " + currUser.Expiration);
                return new HttpResponse(md.CurrentHttpRequest, false, 401, null, "application/json", null, true);
            }

            #endregion

            #region Respond

            currUser.Password = null;
            return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", Common.SerializeJson(currUser), true);

            #endregion
        }
    }
}