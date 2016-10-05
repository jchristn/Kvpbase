using System;
using SyslogLogging;

namespace Kvpbase
{
    public class Token
    {
        #region Public-Members

        public int? UserMasterId { get; set; }
        public string Email { get; set; }
        public string Password { get; set; }
        public string Guid { get; set; }
        public string Random { get; set; }
        public DateTime? Expiration { get; set; }

        #endregion

        #region Constructors-and-Factories

        public Token()
        {

        }

        public static string FromUser(UserMaster curr, Settings settings, EncryptionModule encryption)
        {
            if (curr == null) throw new ArgumentNullException(nameof(curr));
            if (encryption == null) throw new ArgumentNullException(nameof(encryption));

            Token ret = new Token();
            ret.UserMasterId = curr.UserMasterId;
            ret.Email = curr.Email;
            ret.Password = curr.Password;
            ret.Guid = curr.Guid;
            ret.Random = Common.RandomString(8);
            ret.Expiration = DateTime.Now.AddSeconds(settings.Server.TokenExpirationSec);

            string json = Common.SerializeJson(ret);
            return encryption.LocalEncrypt(json);
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Public-Static-Methods

        public static bool VerifyToken(
            string token,
            UserManager users,
            EncryptionModule encryption,
            Events logging,
            out UserMaster currUser,
            out ApiKeyPermission currPerm)
        {
            currUser = new UserMaster();
            currPerm = new ApiKeyPermission();
            
            #region Decrypt-and-Deserialize

            string json = encryption.LocalDecrypt(token);
            Token curr = null;

            try
            {
                curr = Common.DeserializeJson<Token>(json);
            }
            catch (Exception)
            {
                logging.Log(LoggingModule.Severity.Warn, "VerifyToken unable to deserialize request body");
                return false;
            }

            #endregion

            #region Ensure-Token-Not-Expired

            if (!Common.IsLaterThanNow(curr.Expiration))
            {
                logging.Log(LoggingModule.Severity.Warn, "VerifyToken token expired at " + curr.Expiration);
                return false;
            }

            #endregion

            #region Ensure-User-Active-and-Not-Expired

            currUser = users.GetUserById(curr.UserMasterId);
            if (currUser == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "VerifyToken unable to find UserMasterId " + curr.UserMasterId);
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
                    logging.Log(LoggingModule.Severity.Warn, "VerifyToken UserMasterId " + currUser.UserMasterId + " expired at " + curr.Expiration);
                    return false;
                }
            }
            else
            {
                logging.Log(LoggingModule.Severity.Warn, "VerifyToken UserMasterId " + currUser.UserMasterId + " marked inactive");
                return false;
            }

            #endregion
        }

        #endregion
    }
}
