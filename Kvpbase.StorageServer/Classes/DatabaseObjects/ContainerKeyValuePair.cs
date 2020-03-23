using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks; 
using DatabaseWrapper;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// Container key-value pair for metadata.
    /// </summary>
    public class ContainerKeyValuePair
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
        /// Metadata key.
        /// </summary>
        public string MetadataKey { get; set; }

        /// <summary>
        /// Metadata value.
        /// </summary>
        public string MetadataValue { get; set; } 
          
        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public ContainerKeyValuePair()
        {
            GUID = Guid.NewGuid().ToString();
        }
         
        internal ContainerKeyValuePair(string containerGuid, string key, string val)
        {
            if (String.IsNullOrEmpty(containerGuid)) throw new ArgumentNullException(nameof(containerGuid));
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            GUID = Guid.NewGuid().ToString();
            ContainerGUID = containerGuid;
            MetadataKey = key;
            MetadataValue = val;
        }
         
        internal static ContainerKeyValuePair FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            ContainerKeyValuePair ret = new ContainerKeyValuePair();

            if (row.Table.Columns.Contains("id") && row["id"] != null && row["id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["id"]);

            if (row.Table.Columns.Contains("guid") && row["guid"] != null && row["guid"] != DBNull.Value)
                ret.GUID = row["guid"].ToString();

            if (row.Table.Columns.Contains("containerguid") && row["containerguid"] != null && row["containerguid"] != DBNull.Value)
                ret.ContainerGUID = row["containerguid"].ToString();

            if (row.Table.Columns.Contains("metadatakey") && row["metadatakey"] != null && row["metadatakey"] != DBNull.Value)
                ret.MetadataKey = row["metadatakey"].ToString();

            if (row.Table.Columns.Contains("metadatavalue") && row["metadatavalue"] != null && row["metadatavalue"] != DBNull.Value)
                ret.MetadataValue = row["metadatavalue"].ToString();
              
            return ret;
        }

        internal static List<ContainerKeyValuePair> FromDataTable(DataTable table)
        {
            if (table == null) return null;
            List<ContainerKeyValuePair> ret = new List<ContainerKeyValuePair>();
            foreach (DataRow row in table.Rows) ret.Add(FromDataRow(row));
            return ret;
        }

        internal Dictionary<string, object> ToInsertDictionary()
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("guid", GUID);
            ret.Add("containerguid", ContainerGUID);
            ret.Add("metadatakey", MetadataKey);
            ret.Add("metadatavalue", MetadataValue);
            return ret;
        }

        internal static List<Column> GetTableColumns()
        {
            List<Column> ret = new List<Column>();
            ret.Add(new Column("id", true, DataType.Int, 11, null, false));
            ret.Add(new Column("guid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("containerguid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("metadatakey", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("metadatavalue", false, DataType.Nvarchar, 1024, null, true)); 
            return ret;
        } 
    }
}
