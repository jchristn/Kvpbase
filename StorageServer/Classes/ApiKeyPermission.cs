using System;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

namespace Kvpbase
{
    public class ApiKeyPermission
    {
        #region Public-Members

        public int ApiKeyPermissionId { get; set; }
        public int UserMasterId { get; set; }
        public int ApiKeyId { get; set; }
        public string Notes { get; set; }

        public int? AllowReadObject { get; set; }
        public int? AllowReadContainer { get; set; }
        public int? AllowWriteObject { get; set; }
        public int? AllowWriteContainer { get; set; }
        public int? AllowDeleteObject { get; set; }
        public int? AllowDeleteContainer { get; set; }
        public int? AllowSearch { get; set; }

        public string Guid { get; set; }
        public int? Active { get; set; }
        public DateTime? Created { get; set; }
        public DateTime? LastUpdate { get; set; }
        public DateTime? Expiration { get; set; }

        #endregion
        
        #region Constructors-and-Factories

        public ApiKeyPermission()
        {

        }

        public static List<ApiKeyPermission> FromFile(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));
            if (!Common.FileExists(filename)) throw new FileNotFoundException(nameof(filename));

            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("Reading API key permissions from " + filename);
            string contents = Common.ReadTextFile(@filename);

            if (String.IsNullOrEmpty(contents))
            {
                Common.ExitApplication("ApiKeyPermission", "Unable to read contents of " + filename, -1);
                return null;
            }

            Console.WriteLine("Deserializing " + filename);
            List<ApiKeyPermission> ret = null;

            try
            {
                ret = Common.DeserializeJson<List<ApiKeyPermission>>(contents);
                if (ret == null)
                {
                    Common.ExitApplication("ApiKeyPermission", "Unable to deserialize " + filename + " (null)", -1);
                    return null;
                }
            }
            catch (Exception e)
            {
                Events.ExceptionConsole("ApiKeyPermission", "Deserialization issue with " + filename, e);
                Common.ExitApplication("ApiKeyPermission", "Unable to deserialize " + filename + " (exception)", -1);
                return null;
            }

            return ret;
        }

        public static ApiKeyPermission DefaultPermit(UserMaster curr)
        {
            if (curr == null) throw new ArgumentNullException(nameof(curr));
            ApiKeyPermission ret = new ApiKeyPermission();
            ret.ApiKeyPermissionId = 0;
            ret.ApiKeyId = 0;
            ret.UserMasterId = Convert.ToInt32(curr.UserMasterId);
            ret.AllowReadObject = 1;
            ret.AllowReadContainer = 1;
            ret.AllowWriteObject = 1;
            ret.AllowWriteContainer = 1;
            ret.AllowDeleteObject = 1;
            ret.AllowDeleteContainer = 1;
            ret.AllowSearch = 1;
            return ret;
        }

        #endregion

        #region Public-Methods

        #endregion
    }
}
