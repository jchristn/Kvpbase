using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase.Classes.Managers
{
    public class TokenManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private EncryptionManager _Encryption;
        private UserManager _Users;

        #endregion

        #region Constructors-and-Factories

        public TokenManager(Settings settings, LoggingModule logging, EncryptionManager encryption, UserManager users)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (encryption == null) throw new ArgumentNullException(nameof(encryption));
            if (users == null) throw new ArgumentNullException(nameof(users));

            _Settings = settings;
            _Logging = logging;
            _Encryption = encryption;
            _Users = users;
        }

        #endregion

        #region Public-Methods

        public string TokenFromUser(UserMaster curr)
        {
            if (curr == null) throw new ArgumentNullException(nameof(curr));

            Token ret = new Token();
            ret.UserMasterId = curr.UserMasterId;
            ret.Email = curr.Email;
            ret.Password = curr.Password;
            ret.Guid = curr.Guid;
            ret.Random = Common.RandomString(8);
            ret.Expiration = DateTime.Now.AddSeconds(_Settings.Server.TokenExpirationSec);

            string json = Common.SerializeJson(ret, false);
            return _Encryption.LocalEncrypt(json);
        }

        public bool VerifyToken(
            string token, 
            out UserMaster currUser,
            out ApiKeyPermission currPerm)
        {
            currUser = new UserMaster();
            currPerm = new ApiKeyPermission();

            #region Decrypt-and-Deserialize

            string json = _Encryption.LocalDecrypt(token);
            Token curr = null;

            try
            {
                curr = Common.DeserializeJson<Token>(json);
            }
            catch (Exception)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "VerifyToken unable to deserialize request body");
                return false;
            }

            #endregion

            #region Ensure-Token-Not-Expired

            if (!Common.IsLaterThanNow(curr.Expiration))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "VerifyToken token expired at " + curr.Expiration);
                return false;
            }

            #endregion

            #region Ensure-User-Active-and-Not-Expired

            currUser = _Users.GetUserById(curr.UserMasterId);
            if (currUser == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "VerifyToken unable to find UserMasterId " + curr.UserMasterId);
                return false;
            }

            currPerm = ApiKeyPermission.DefaultPermit(currUser);

            if (currUser.Active == 1)
            {
                if (Common.IsLaterThanNow(currUser.Expiration))
                {
                    return true;
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "VerifyToken UserMasterId " + currUser.UserMasterId + " expired at " + curr.Expiration);
                    return false;
                }
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "VerifyToken UserMasterId " + currUser.UserMasterId + " marked inactive");
                return false;
            }

            #endregion
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
