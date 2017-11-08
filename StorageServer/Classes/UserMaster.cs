using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

namespace Kvpbase
{
    public class UserMaster
    {
        #region Public-Members

        public int? UserMasterId { get; set; }
        public int? NodeId { get; set; }
        public int? ExpirationSec { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string CompanyName { get; set; }
        public string Email { get; set; }
        public string Password { get; set; }
        public string Cellphone { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string PostalCode { get; set; }
        public string Country { get; set; }

        public int? GatewayMode { get; set; }
        public string HomeDirectory { get; set; }
        public int? DefaultCompress { get; set; }
        public int? DefaultEncrypt { get; set; }

        public int? IsAdmin { get; set; }
        public string Guid { get; set; }
        public int? Active { get; set; }
        public DateTime? Created { get; set; }
        public DateTime? LastUpdate { get; set; }
        public DateTime? Expiration { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public UserMaster()
        {

        }

        public static List<UserMaster> FromFile(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));
            if (!Common.FileExists(filename)) throw new FileNotFoundException(nameof(filename));

            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("Reading users from " + filename);
            string contents = Common.ReadTextFile(@filename);

            if (String.IsNullOrEmpty(contents))
            {
                Common.ExitApplication("UserMaster", "Unable to read contents of " + filename, -1);
                return null;
            }

            Console.WriteLine("Deserializing " + filename);
            List<UserMaster> ret = null;

            try
            {
                ret = Common.DeserializeJson<List<UserMaster>>(contents);
                if (ret == null)
                {
                    Common.ExitApplication("UserMaster", "Unable to deserialize " + filename + " (null)", -1);
                    return null;
                }
            }
            catch (Exception e)
            {
                Events.ExceptionConsole("UserMaster", "Deserialization issue with " + filename, e);
                Common.ExitApplication("UserMaster", "Unable to deserialize " + filename + " (exception)", -1);
                return null;
            }

            return ret;
        }

        public static UserMaster FromPath(string path, Settings settings, UserManager users, out string homeDirectory, out bool isGlobal)
        {
            isGlobal = false;
            homeDirectory = "";

            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(path)) return null;

            #endregion

            #region Variables

            UserMaster ret = null;
            string directoryName = "";
            string filenameWithExtension = "";
            string filenameWithoutExtension = "";
            string fileExtension = "";
            string userGuid = "";

            List<UserMaster> matchingUsers = new List<UserMaster>();

            #endregion

            #region Parse-Path

            directoryName = Path.GetDirectoryName(path) + Common.GetPathSeparator(settings.Environment);
            filenameWithExtension = Path.GetFileName(path);
            filenameWithoutExtension = Path.GetFileNameWithoutExtension(path);
            fileExtension = Path.GetExtension(path);

            #endregion

            #region Compare-Against-Global

            if (directoryName.ToLower().StartsWith(settings.Storage.Directory.ToLower()))
            {
                #region Global-Storage-Directory

                homeDirectory = String.Copy(settings.Storage.Directory.ToLower());
                isGlobal = true;

                // get user GUID and user
                userGuid = String.Copy(path);
                userGuid = userGuid.Replace(homeDirectory + Common.GetPathSeparator(settings.Environment), "");
                userGuid = userGuid.Replace(homeDirectory, "");

                List<string> dirElements = new List<string>(userGuid.Split(Convert.ToChar(Common.GetPathSeparator(settings.Environment))));
                if (dirElements != null && dirElements.Count > 0)
                {
                    foreach (string currDirElement in dirElements)
                    {
                        userGuid = String.Copy(currDirElement);
                        break;
                    }
                }

                ret = users.GetUserByGuid(userGuid);
                homeDirectory = String.Copy(settings.Storage.Directory.ToLower()) + userGuid + Common.GetPathSeparator(settings.Environment);

                #endregion
            }

            #endregion

            #region Compare-Against-Users

            matchingUsers = users.GetUsersByHomeDirectory(directoryName);
            if (matchingUsers != null && matchingUsers.Count > 0)
            {
                foreach (UserMaster curr in matchingUsers)
                {
                    if (curr.HomeDirectory.Length >= homeDirectory.Length)
                    {
                        homeDirectory = String.Copy(curr.HomeDirectory);
                        ret = curr;
                        isGlobal = false;
                    }
                }
            }

            #endregion

            return ret;
        }

        #endregion

        #region Public-Methods

        public bool GetGatewayMode(Settings curr)
        {
            if (GatewayMode != null)
            {
                return Common.IsTrue(GatewayMode);
            }
            else if (curr != null && curr.Storage != null)
            {
                return Common.IsTrue(curr.Storage.GatewayMode);
            }
            else
            {
                return false;
            }
        }

        public bool GetCompressionMode(Settings curr)
        {
            if (GatewayMode == null) return false;
            if (GatewayMode == 1) return false;
            if (DefaultCompress != null)
            {
                if (DefaultCompress == 1) return true;
                return false;
            }

            if (Common.IsTrue(curr.Storage.GatewayMode))
            {
                return false;
            }
            else
            {
                return Common.IsTrue(curr.Storage.DefaultCompress);
            }
        }

        public bool GetEncryptionMode(Settings curr)
        {
            if (GatewayMode == null) return false;
            if (GatewayMode == 1) return false;
            if (DefaultEncrypt != null)
            {
                if (DefaultEncrypt == 1) return true;
                return false;
            }

            if (Common.IsTrue(curr.Storage.GatewayMode))
            {
                return false;
            }
            else
            {
                return Common.IsTrue(curr.Storage.DefaultEncrypt);
            }
        }

        public int GetExpirationSeconds(Settings settings, ApiKey apiKey)
        {
            if (apiKey != null)
            {
                if (apiKey.ExpirationSec != null && apiKey.ExpirationSec > 0)
                {
                    return Convert.ToInt32(apiKey.ExpirationSec);
                }
            }

            if (ExpirationSec != null && ExpirationSec >= 0)
            {
                return Convert.ToInt32(ExpirationSec);
            }

            if (settings.Expiration.DefaultExpirationSec >= 0)
            {
                return settings.Expiration.DefaultExpirationSec;
            }

            return 0;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
