using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

using DatabaseWrapper;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// API key for use when accessing the RESTful HTTP API.
    /// </summary>
    public class ApiKey
    { 
        /// <summary>
        /// Row ID in the database.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Object GUID.  This value is used as the actual API key.
        /// </summary>
        public string GUID { get; set; }
         
        /// <summary>
        /// User GUID.
        /// </summary>
        public string UserGUID { get; set; }
          
        /// <summary>
        /// Indicates if the account is active or disabled.
        /// </summary>
        public bool Active { get; set; }
          
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public ApiKey()
        {
            GUID = Guid.NewGuid().ToString();
        }
         
        internal static ApiKey FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            ApiKey ret = new ApiKey();
             
            if (row.Table.Columns.Contains("id") && row["id"] != null && row["id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["id"]);

            if (row.Table.Columns.Contains("guid") && row["guid"] != null && row["guid"] != DBNull.Value)
                ret.GUID = row["guid"].ToString();

            if (row.Table.Columns.Contains("userguid") && row["userguid"] != null && row["userguid"] != DBNull.Value)
                ret.UserGUID = row["userguid"].ToString();
 
            if (row.Table.Columns.Contains("active") && row["active"] != null && row["active"] != DBNull.Value)
                ret.Active = Convert.ToBoolean(row["active"]);

            return ret;
        }
         
        internal static List<ApiKey> FromDataTable(DataTable table)
        {
            if (table == null) return null;
            List<ApiKey> ret = new List<ApiKey>();
            foreach (DataRow row in table.Rows) ret.Add(FromDataRow(row));
            return ret;
        }

        internal Dictionary<string, object> ToInsertDictionary()
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("guid", GUID);
            ret.Add("userguid", UserGUID); 
            ret.Add("active", Active); 
            return ret;
        }
         
        internal static List<Column> GetTableColumns()
        {
            List<Column> ret = new List<Column>();
            ret.Add(new Column("id", true, DataType.Int, 11, null, false));
            ret.Add(new Column("guid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("userguid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("active", false, DataType.Nvarchar, 8, null, false));
            return ret;
        } 
    }
}
