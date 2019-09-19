using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DatabaseWrapper;

namespace Kvpbase.Classes
{
    /// <summary>
    /// Container key-value pair for metadata.
    /// </summary>
    public class ContainerKeyValuePair
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
        /// Metadata key.
        /// </summary>
        public string MetaKey { get; set; }

        /// <summary>
        /// Metadata value.
        /// </summary>
        public string MetaValue { get; set; } 

        /// <summary>
        /// The timestamp from when the object was created.
        /// </summary>
        public DateTime? CreatedUtc { get; set; }

        #endregion

        #region Private-Members
         
        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public ContainerKeyValuePair()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public ContainerKeyValuePair(string key, string val)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime();
            MetaKey = key;
            MetaValue = val;
        }

        /// <summary>
        /// Instantiate the object from a DataRow.
        /// </summary>
        /// <param name="row">DataRow.</param>
        /// <returns>ContainerKeyValuePair.</returns>
        public static ContainerKeyValuePair FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            ContainerKeyValuePair ret = new ContainerKeyValuePair();

            if (row.Table.Columns.Contains("Id") && row["Id"] != null && row["Id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["Id"]);

            if (row.Table.Columns.Contains("GUID") && row["GUID"] != null && row["GUID"] != DBNull.Value)
                ret.GUID = row["GUID"].ToString();

            if (row.Table.Columns.Contains("MetaKey") && row["MetaKey"] != null && row["MetaKey"] != DBNull.Value)
                ret.MetaKey = row["MetaKey"].ToString();

            if (row.Table.Columns.Contains("MetaValue") && row["MetaValue"] != null && row["MetaValue"] != DBNull.Value)
                ret.MetaValue = row["MetaValue"].ToString();
             
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
            ret.Add("MetaKey", MetaKey);
            ret.Add("MetaValue", MetaValue);
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
            ret.Add(new Column("MetaKey", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("MetaValue", false, DataType.Nvarchar, 1024, null, true));
            ret.Add(new Column("CreatedUtc", false, DataType.DateTime, 32, null, false));
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
