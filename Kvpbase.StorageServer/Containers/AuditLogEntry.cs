using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DatabaseWrapper;

namespace Kvpbase.Containers
{
    /// <summary>
    /// Details about an audit log entry.
    /// </summary>
    public class AuditLogEntry
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
        /// Key, if any, associated with the action.
        /// </summary>
        public string ObjectKey { get; set; }

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
        public DateTime? CreatedUtc { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public AuditLogEntry()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <param name="action">Action performed.</param>
        /// <param name="metadata">Request metadata.</param>
        public AuditLogEntry(string key, AuditLogEntryType action, string metadata)
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime();
            ObjectKey = key;
            Action = action;
            Metadata = metadata;
        }

        /// <summary>
        /// Instantiate the object from a DataRow.
        /// </summary>
        /// <param name="row">DataRow.</param>
        /// <returns>AuditLogEntry.</returns>
        public static AuditLogEntry FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            AuditLogEntry ret = new AuditLogEntry();

            if (row.Table.Columns.Contains("Id") && row["Id"] != null && row["Id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["Id"]);

            if (row.Table.Columns.Contains("GUID") && row["GUID"] != null && row["GUID"] != DBNull.Value)
                ret.GUID = row["GUID"].ToString();

            if (row.Table.Columns.Contains("ObjectKey") && row["ObjectKey"] != null && row["ObjectKey"] != DBNull.Value)
                ret.ObjectKey = row["ObjectKey"].ToString();

            if (row.Table.Columns.Contains("Action") && row["Action"] != null && row["Action"] != DBNull.Value)
                ret.Action = (AuditLogEntryType)(Enum.Parse(typeof(AuditLogEntryType), row["Action"].ToString()));

            if (row.Table.Columns.Contains("Metadata") && row["Metadata"] != null && row["Metadata"] != DBNull.Value)
                ret.Metadata = row["Metadata"].ToString();

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
            ret.Add("ObjectKey", ObjectKey);
            ret.Add("Action", Action.ToString());
            ret.Add("Metadata", Metadata);
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
            ret.Add(new Column("ObjectKey", false, DataType.Nvarchar, 256, null, true));
            ret.Add(new Column("Action", false, DataType.Nvarchar, 32, null, true));
            ret.Add(new Column("Metadata", false, DataType.Nvarchar, 256, null, true));
            ret.Add(new Column("CreatedUtc", false, DataType.DateTime, 32, null, false)); 
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion

    }
}
