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
    /// Object container.
    /// </summary>
    public class Container
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
        /// The username of the owner.
        /// </summary>
        public string UserGUID { get; set; }

        /// <summary>
        /// The name of the container.
        /// </summary>
        public string Name { get; set; }
         
        /// <summary>
        /// The full path to where container objects should be stored.
        /// </summary>
        public string ObjectsDirectory { get; set; }
         
        /// <summary>
        /// Enable or disable audit logging.
        /// </summary>
        public bool EnableAuditLogging { get; set; }

        /// <summary>
        /// Enable or disable public read access.
        /// </summary>
        public bool IsPublicRead { get; set; }

        /// <summary>
        /// Enable or disable public write access.
        /// </summary>
        public bool IsPublicWrite { get; set; }

        /// <summary>
        /// The timestamp from when the object was created.
        /// </summary>
        public DateTime? CreatedUtc { get; set; }
         
        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public Container()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }
         
        internal static Container FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            Container ret = new Container();

            if (row.Table.Columns.Contains("id") && row["id"] != null && row["id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["id"]);

            if (row.Table.Columns.Contains("guid") && row["guid"] != null && row["guid"] != DBNull.Value)
                ret.GUID = row["guid"].ToString();

            if (row.Table.Columns.Contains("userguid") && row["userguid"] != null && row["userguid"] != DBNull.Value)
                ret.UserGUID = row["userguid"].ToString();

            if (row.Table.Columns.Contains("name") && row["name"] != null && row["name"] != DBNull.Value)
                ret.Name = row["name"].ToString();

            if (row.Table.Columns.Contains("objectsdirectory") && row["objectsdirectory"] != null && row["objectsdirectory"] != DBNull.Value)
                ret.ObjectsDirectory = row["objectsdirectory"].ToString();
             
            if (row.Table.Columns.Contains("enableauditlogging") && row["enableauditlogging"] != null && row["enableauditlogging"] != DBNull.Value)
                ret.EnableAuditLogging = Convert.ToBoolean(row["enableauditlogging"]);

            if (row.Table.Columns.Contains("ispublicread") && row["ispublicread"] != null && row["ispublicread"] != DBNull.Value)
                ret.IsPublicRead = Convert.ToBoolean(row["ispublicread"]);

            if (row.Table.Columns.Contains("ispublicwrite") && row["ispublicwrite"] != null && row["ispublicwrite"] != DBNull.Value)
                ret.IsPublicWrite = Convert.ToBoolean(row["ispublicwrite"]);

            if (row.Table.Columns.Contains("createdutc") && row["createdutc"] != null && row["createdutc"] != DBNull.Value)
                ret.CreatedUtc = Convert.ToDateTime(row["createdutc"]);

            return ret;
        }

        internal static List<Container> FromDataTable(DataTable table)
        {
            if (table == null) return null;
            List<Container> ret = new List<Container>();
            foreach (DataRow row in table.Rows) ret.Add(FromDataRow(row));
            return ret;
        }

        internal Dictionary<string, object> ToInsertDictionary()
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("guid", GUID);
            ret.Add("userguid", UserGUID);
            ret.Add("name", Name);
            ret.Add("objectsdirectory", ObjectsDirectory); 
            ret.Add("enableauditlogging", EnableAuditLogging.ToString());
            ret.Add("ispublicread", IsPublicRead.ToString());
            ret.Add("ispublicwrite", IsPublicWrite.ToString());
            ret.Add("createdutc", CreatedUtc);
            return ret;
        }

        internal static List<Column> GetTableColumns()
        {
            List<Column> ret = new List<Column>();
            ret.Add(new Column("id", true, DataType.Int, 11, null, false));
            ret.Add(new Column("guid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("userguid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("name", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("objectsdirectory", false, DataType.Nvarchar, 256, null, false));
            ret.Add(new Column("enableauditlogging", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("ispublicread", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("ispublicwrite", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("createdutc", false, DataType.DateTime, 32, null, false)); 
            return ret;
        } 
    }
}
