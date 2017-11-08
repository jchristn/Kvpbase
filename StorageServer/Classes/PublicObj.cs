using System;
using SyslogLogging;

namespace Kvpbase
{
    public class PublicObj
    {
        #region Public-Members

        public string Guid { get; set; }
        public string Url { get; set; }
        public string DiskPath { get; set; }
        public int IsObject { get; set; }
        public int IsContainer { get; set; }
        public DateTime Created { get; set; }
        public DateTime Expiration { get; set; }
        public string UserGuid { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public PublicObj()
        {

        }

        #endregion

        #region Public-Methods

        public static string BuildUrl(string guid, Node curr)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (curr == null) throw new ArgumentNullException(nameof(curr));

            string url = "";
            if (Common.IsTrue(curr.Ssl)) url += "https://";
            else url += "http://";
            url += curr.DnsHostname + ":" + curr.Port + "/public/" + guid;

            return url;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
