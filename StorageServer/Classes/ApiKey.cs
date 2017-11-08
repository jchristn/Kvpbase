using System;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

namespace Kvpbase
{
    public class ApiKey
    {
        #region Public-Members

        public int? ApiKeyId { get; set; }
        public int? UserMasterId { get; set; }
        public int? ExpirationSec { get; set; }
        public string Guid { get; set; }
        public string Notes { get; set; }
        public int? Active { get; set; }
        public DateTime? Created { get; set; }
        public DateTime? LastUpdate { get; set; }
        public DateTime? Expiration { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public ApiKey()
        {

        }

        public static List<ApiKey> FromFile(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));
            if (!Common.FileExists(filename)) throw new FileNotFoundException(nameof(filename));

            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("Reading API keys from " + filename);
            string contents = Common.ReadTextFile(@filename);

            if (String.IsNullOrEmpty(contents))
            {
                Common.ExitApplication("ApiKey", "Unable to read contents of " + filename, -1);
                return null;
            }

            Console.WriteLine("Deserializing " + filename);
            List<ApiKey> ret = null;

            try
            {
                ret = Common.DeserializeJson<List<ApiKey>>(contents);
                if (ret == null)
                {
                    Common.ExitApplication("ApiKey", "Unable to deserialize " + filename + " (null)", -1);
                    return null;
                }
            }
            catch (Exception e)
            {
                Events.ExceptionConsole("ApiKey", "Deserialization issue with " + filename, e);
                Common.ExitApplication("ApiKey", "Unable to deserialize " + filename + " (exception)", -1);
                return null;
            }

            return ret;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
