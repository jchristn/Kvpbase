using System;
using System.Collections.Generic;
using System.Data;
using System.Net;
using System.Text;

using DatabaseWrapper;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// A lock applied to a URL for an in-progress operation.
    /// </summary>
    public class UrlLock
    { 
        /// <summary>
        /// Row ID in the database.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// GUID.
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
        public string UserGUID { get; set; }
         
        /// <summary>
        /// The hostname upon which the lock was generated.
        /// </summary>
        public string Hostname { get; set; }

        /// <summary>
        /// The timestamp from when the object was created.
        /// </summary>
        public DateTime CreatedUtc { get; set; }
         
        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public UrlLock()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime(); 
        }
         
        internal UrlLock(LockType lockType, string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url)); 

            GUID = Guid.NewGuid().ToString();
            LockType = lockType;
            Url = url;
            UserGUID = userGuid; 
            Hostname = Dns.GetHostName();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }
         
        internal static UrlLock FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            UrlLock ret = new UrlLock();

            if (row.Table.Columns.Contains("id") && row["id"] != null && row["id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["id"]);

            if (row.Table.Columns.Contains("guid") && row["guid"] != null && row["guid"] != DBNull.Value)
                ret.GUID = row["guid"].ToString();

            if (row.Table.Columns.Contains("locktype") && row["locktype"] != null && row["locktype"] != DBNull.Value)
                ret.LockType = (LockType)(Enum.Parse(typeof(LockType), row["locktype"].ToString()));

            if (row.Table.Columns.Contains("url") && row["url"] != null && row["url"] != DBNull.Value)
                ret.Url = row["url"].ToString();

            if (row.Table.Columns.Contains("userguid") && row["userguid"] != null && row["userguid"] != DBNull.Value)
                ret.UserGUID = row["userguid"].ToString();
             
            if (row.Table.Columns.Contains("hostname") && row["hostname"] != null && row["hostname"] != DBNull.Value)
                ret.Hostname = row["hostname"].ToString();

            if (row.Table.Columns.Contains("createdutc") && row["createdutc"] != null && row["createdutc"] != DBNull.Value)
                ret.CreatedUtc = Convert.ToDateTime(row["createdutc"]);

            return ret;
        }

        internal static List<UrlLock> FromDataTable(DataTable table)
        {
            if (table == null) return null;
            List<UrlLock> ret = new List<UrlLock>();
            foreach (DataRow row in table.Rows) ret.Add(FromDataRow(row));
            return ret;
        }

        internal Dictionary<string, object> ToInsertDictionary()
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("guid", GUID);
            ret.Add("locktype", LockType.ToString());
            ret.Add("url", Url);
            ret.Add("userguid", UserGUID); 
            ret.Add("hostname", Hostname);
            ret.Add("createdutc", CreatedUtc); 
            return ret;
        }
         
        internal static List<Column> GetTableColumns()
        {
            List<Column> ret = new List<Column>();
            ret.Add(new Column("id", true, DataType.Int, 11, null, false));
            ret.Add(new Column("guid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("locktype", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("url", false, DataType.Nvarchar, 256, null, false));
            ret.Add(new Column("userguid", false, DataType.Nvarchar, 64, null, true)); 
            ret.Add(new Column("hostname", false, DataType.Nvarchar, 256, null, false));
            ret.Add(new Column("createdutc", false, DataType.DateTime, 32, null, false));
            return ret;
        } 
    }
}
