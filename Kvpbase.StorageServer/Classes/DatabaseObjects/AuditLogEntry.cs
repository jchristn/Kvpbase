using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper; 

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// Details about an audit log entry.
    /// </summary>
    public class AuditLogEntry
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
        /// Container GUID.
        /// </summary>
        public string ContainerGUID { get; set; }
         
        /// <summary>
        /// Object GUID.
        /// </summary>
        public string ObjectGUID { get; set; }
         
        /// <summary>
        /// Action performed by the requestor.
        /// </summary>
        public AuditLogEntryType Action { get; set; }

        /// <summary>
        /// Metadata associated with the action.
        /// </summary>
        public string Metadata { get; set; }

        /// <summary>
        /// Timestamp of the action.
        /// </summary>
        public DateTime CreatedUtc { get; set; }
         
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public AuditLogEntry()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }
         
        internal AuditLogEntry(string containerGuid, string objectGuid, AuditLogEntryType action, string metadata)
        {
            GUID = Guid.NewGuid().ToString();
            ContainerGUID = containerGuid; 
            ObjectGUID = objectGuid; 
            CreatedUtc = DateTime.Now.ToUniversalTime(); 
            Action = action;
            Metadata = metadata;
        }
         
        internal static AuditLogEntry FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            AuditLogEntry ret = new AuditLogEntry();

            if (row.Table.Columns.Contains("id") && row["id"] != null && row["id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["id"]);

            if (row.Table.Columns.Contains("guid") && row["guid"] != null && row["guid"] != DBNull.Value)
                ret.GUID = row["guid"].ToString();

            if (row.Table.Columns.Contains("containerguid") && row["containerguid"] != null && row["containerguid"] != DBNull.Value)
                ret.ContainerGUID = row["containerguid"].ToString();
             
            if (row.Table.Columns.Contains("objectguid") && row["objectguid"] != null && row["objectguid"] != DBNull.Value)
                ret.ObjectGUID = row["objectguid"].ToString();
             
            if (row.Table.Columns.Contains("action") && row["action"] != null && row["action"] != DBNull.Value)
                ret.Action = (AuditLogEntryType)(Enum.Parse(typeof(AuditLogEntryType), row["action"].ToString()));

            if (row.Table.Columns.Contains("metadata") && row["metadata"] != null && row["metadata"] != DBNull.Value)
                ret.Metadata = row["metadata"].ToString();

            if (row.Table.Columns.Contains("createdutc") && row["createdutc"] != null && row["createdutc"] != DBNull.Value)
                ret.CreatedUtc = Convert.ToDateTime(row["createdutc"]);

            return ret;
        }

        internal static List<AuditLogEntry> FromDataTable(DataTable table)
        {
            if (table == null) return null;
            List<AuditLogEntry> ret = new List<AuditLogEntry>();
            foreach (DataRow row in table.Rows) ret.Add(FromDataRow(row));
            return ret;
        }

        internal Dictionary<string, object> ToInsertDictionary()
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("guid", GUID);
            ret.Add("containerguid", ContainerGUID); 
            ret.Add("objectguid", ObjectGUID); 
            ret.Add("action", Action.ToString());
            ret.Add("metadata", Metadata);
            ret.Add("createdutc", CreatedUtc); 
            return ret; 
        }
         
        internal static List<Column> GetTableColumns()
        {
            List<Column> ret = new List<Column>();
            ret.Add(new Column("id", true, DataType.Int, 11, null, false));
            ret.Add(new Column("guid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("containerguid", false, DataType.Nvarchar, 64, null, false)); 
            ret.Add(new Column("objectguid", false, DataType.Nvarchar, 64, null, false)); 
            ret.Add(new Column("action", false, DataType.Nvarchar, 32, null, true));
            ret.Add(new Column("metadata", false, DataType.Nvarchar, 256, null, true));
            ret.Add(new Column("createdutc", false, DataType.DateTime, 32, null, false)); 
            return ret;
        } 
    }
}
