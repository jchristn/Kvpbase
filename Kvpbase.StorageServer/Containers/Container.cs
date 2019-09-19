using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DatabaseWrapper;

namespace Kvpbase.Containers
{
    /// <summary>
    /// Object container.
    /// </summary>
    public class Container
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
        /// The username of the ontainer owner.
        /// </summary>
        public string UserGuid { get; set; }

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

        #endregion

        #region Private-Members
         
        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public Container()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }

        /// <summary>
        /// Instantiate the object from a DataRow.
        /// </summary>
        /// <param name="row">DataRow.</param>
        /// <returns>Container.</returns>
        public static Container FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            Container ret = new Container();

            if (row.Table.Columns.Contains("Id") && row["Id"] != null && row["Id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["Id"]);

            if (row.Table.Columns.Contains("GUID") && row["GUID"] != null && row["GUID"] != DBNull.Value)
                ret.GUID = row["GUID"].ToString();

            if (row.Table.Columns.Contains("UserGuid") && row["UserGuid"] != null && row["UserGuid"] != DBNull.Value)
                ret.UserGuid = row["UserGuid"].ToString();

            if (row.Table.Columns.Contains("Name") && row["Name"] != null && row["Name"] != DBNull.Value)
                ret.Name = row["Name"].ToString();

            if (row.Table.Columns.Contains("ObjectsDirectory") && row["ObjectsDirectory"] != null && row["ObjectsDirectory"] != DBNull.Value)
                ret.ObjectsDirectory = row["ObjectsDirectory"].ToString();
             
            if (row.Table.Columns.Contains("EnableAuditLogging") && row["EnableAuditLogging"] != null && row["EnableAuditLogging"] != DBNull.Value)
                ret.EnableAuditLogging = Convert.ToBoolean(row["EnableAuditLogging"]);

            if (row.Table.Columns.Contains("IsPublicRead") && row["IsPublicRead"] != null && row["IsPublicRead"] != DBNull.Value)
                ret.IsPublicRead = Convert.ToBoolean(row["IsPublicRead"]);

            if (row.Table.Columns.Contains("IsPublicWrite") && row["IsPublicWrite"] != null && row["IsPublicWrite"] != DBNull.Value)
                ret.IsPublicWrite = Convert.ToBoolean(row["IsPublicWrite"]);
             
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
            ret.Add("UserGuid", UserGuid);
            ret.Add("Name", Name);
            ret.Add("ObjectsDirectory", ObjectsDirectory); 
            ret.Add("EnableAuditLogging", EnableAuditLogging.ToString());
            ret.Add("IsPublicRead", IsPublicRead.ToString());
            ret.Add("IsPublicWrite", IsPublicWrite.ToString());
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
            ret.Add(new Column("UserGuid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("Name", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("ObjectsDirectory", false, DataType.Nvarchar, 256, null, false));
            ret.Add(new Column("EnableAuditLogging", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("IsPublicRead", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("IsPublicWrite", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("CreatedUtc", false, DataType.DateTime, 32, null, false)); 
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
