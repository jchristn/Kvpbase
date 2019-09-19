using System;
using System.Collections.Generic;
using System.Data;
using System.Net;
using System.Text;

using DatabaseWrapper;

namespace Kvpbase.Classes
{
    /// <summary>
    /// A lock applied to a URL for an in-progress operation.
    /// </summary>
    public class UrlLock
    {
        #region Public-Members

        /// <summary>
        /// Row ID in the database.
        /// </summary>
        public int? Id { get; set; }

        /// <summary>
        /// Object GUID.
        /// </summary>
        public string GUID { get; set; }

        /// <summary>
        /// The lock type.
        /// </summary>
        public LockType LockType { get; set; }

        /// <summary>
        /// The raw URL that is locked.
        /// </summary>
        public string Url { get; set; }

        /// <summary>
        /// The GUID of the user that holds the lock.
        /// </summary>
        public string UserGuid { get; set; }

        /// <summary>
        /// The hostname upon which the lock was generated.
        /// </summary>
        public string Hostname { get; set; }

        /// <summary>
        /// The timestamp from when the object was created.
        /// </summary>
        public DateTime CreatedUtc { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public UrlLock()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime(); 
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="lockType">Type of lock.</param>
        /// <param name="url">Raw URL.</param>
        /// <param name="userGuid">User GUID.</param>
        public UrlLock(LockType lockType, string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url)); 

            GUID = Guid.NewGuid().ToString();
            LockType = lockType;
            Url = url;
            UserGuid = userGuid;
            Hostname = Dns.GetHostName();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }

        /// <summary>
        /// Instantiate the object from a DataRow.
        /// </summary>
        /// <param name="row">DataRow.</param>
        /// <returns>UrlLock.</returns>
        public static UrlLock FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            UrlLock ret = new UrlLock();

            if (row.Table.Columns.Contains("Id") && row["Id"] != null && row["Id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["Id"]);

            if (row.Table.Columns.Contains("GUID") && row["GUID"] != null && row["GUID"] != DBNull.Value)
                ret.GUID = row["GUID"].ToString();

            if (row.Table.Columns.Contains("LockType") && row["LockType"] != null && row["LockType"] != DBNull.Value)
                ret.LockType = (LockType)(Enum.Parse(typeof(LockType), row["LockType"].ToString()));

            if (row.Table.Columns.Contains("Url") && row["Url"] != null && row["Url"] != DBNull.Value)
                ret.Url = row["Url"].ToString();

            if (row.Table.Columns.Contains("UserGuid") && row["UserGuid"] != null && row["UserGuid"] != DBNull.Value)
                ret.UserGuid = row["UserGuid"].ToString();

            if (row.Table.Columns.Contains("Hostname") && row["Hostname"] != null && row["Hostname"] != DBNull.Value)
                ret.Hostname = row["Hostname"].ToString();

            if (row.Table.Columns.Contains("CreatedUtc") && row["CreatedUtc"] != null && row["CreatedUtc"] != DBNull.Value)
                ret.CreatedUtc = Convert.ToDateTime(row["CreatedUtc"]);

            return ret;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Create a Dictionary from the object.
        /// </summary>
        /// <returns>Dictionary.</returns>
        public Dictionary<string, object> ToInsertDictionary()
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("GUID", GUID);
            ret.Add("LockType", LockType.ToString());
            ret.Add("Url", Url);
            ret.Add("UserGuid", UserGuid);
            ret.Add("Hostname", Hostname);
            ret.Add("CreatedUtc", CreatedUtc); 
            return ret;
        }

        /// <summary>
        /// Retrieve the list of columns required to create the table.
        /// </summary>
        /// <returns>List of columns.</returns>
        public static List<Column> GetTableColumns()
        {
            List<Column> ret = new List<Column>();
            ret.Add(new Column("Id", true, DataType.Int, 11, null, false));
            ret.Add(new Column("GUID", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("LockType", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("Url", false, DataType.Nvarchar, 256, null, false));
            ret.Add(new Column("UserGuid", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("Hostname", false, DataType.Nvarchar, 256, null, false));
            ret.Add(new Column("CreatedUtc", false, DataType.DateTime, 32, null, false));
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
