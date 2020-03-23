using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

using DatabaseWrapper;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// Permissions associated with a user or API key.
    /// </summary>
    public class Permission
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
        /// GUID of the associated user.
        /// </summary>
        public string UserGUID { get; set; }

        /// <summary>
        /// GUID of the associated API key.
        /// </summary>
        public string ApiKeyGUID { get; set; }

        /// <summary>
        /// Administrator notes for the API key.
        /// </summary>
        public string Notes { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to read objects.
        /// </summary>
        public bool ReadObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to read containers.
        /// </summary>
        public bool ReadContainer { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to write objects.
        /// </summary>
        public bool WriteObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to write containers.
        /// </summary>
        public bool WriteContainer { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to delete objects.
        /// </summary>
        public bool DeleteObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to delete containers.
        /// </summary>
        public bool DeleteContainer { get; set; }

        /// <summary>
        /// Indicates if the object is active or disabled.
        /// </summary>
        public bool Active { get; set; } 
         
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public Permission()
        { 
        }
         
        internal static Permission FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            Permission ret = new Permission();

            if (row.Table.Columns.Contains("id") && row["id"] != null && row["id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["id"]);

            if (row.Table.Columns.Contains("guid") && row["guid"] != null && row["guid"] != DBNull.Value)
                ret.GUID = row["guid"].ToString();

            if (row.Table.Columns.Contains("containerguid") && row["containerguid"] != null && row["containerguid"] != DBNull.Value)
                ret.ContainerGUID = row["containerguid"].ToString();

            if (row.Table.Columns.Contains("userguid") && row["userguid"] != null && row["userguid"] != DBNull.Value)
                ret.UserGUID = row["userguid"].ToString();

            if (row.Table.Columns.Contains("apikeyguid") && row["apikeyguid"] != null && row["apikeyguid"] != DBNull.Value)
                ret.ApiKeyGUID = row["apikeyguid"].ToString();

            if (row.Table.Columns.Contains("notes") && row["notes"] != null && row["notes"] != DBNull.Value)
                ret.Notes = row["notes"].ToString();
             
            if (row.Table.Columns.Contains("readobject") && row["readobject"] != null && row["readobject"] != DBNull.Value)
                ret.ReadObject = Convert.ToBoolean(row["readobject"]);

            if (row.Table.Columns.Contains("readcontainer") && row["readcontainer"] != null && row["readcontainer"] != DBNull.Value)
                ret.ReadContainer = Convert.ToBoolean(row["readcontainer"]);

            if (row.Table.Columns.Contains("writeobject") && row["writeobject"] != null && row["writeobject"] != DBNull.Value)
                ret.WriteObject = Convert.ToBoolean(row["writeobject"]);

            if (row.Table.Columns.Contains("writecontainer") && row["writecontainer"] != null && row["writecontainer"] != DBNull.Value)
                ret.WriteContainer = Convert.ToBoolean(row["writecontainer"]);

            if (row.Table.Columns.Contains("deleteobject") && row["deleteobject"] != null && row["deleteobject"] != DBNull.Value)
                ret.DeleteObject = Convert.ToBoolean(row["deleteobject"]);

            if (row.Table.Columns.Contains("deletecontainer") && row["deletecontainer"] != null && row["deletecontainer"] != DBNull.Value)
                ret.DeleteContainer = Convert.ToBoolean(row["deletecontainer"]);

            if (row.Table.Columns.Contains("active") && row["active"] != null && row["active"] != DBNull.Value)
                ret.Active = Convert.ToBoolean(row["active"]);
             
            return ret;
        }

        internal static List<Permission> FromDataTable(DataTable table)
        {
            if (table == null) return null;
            List<Permission> ret = new List<Permission>();
            foreach (DataRow row in table.Rows) ret.Add(FromDataRow(row));
            return ret;
        }

        internal static Permission DefaultPermit(UserMaster user)
        {
            Permission ret = new Permission();
            ret.Id = 0;
            ret.GUID = Guid.NewGuid().ToString();
            ret.ContainerGUID = "*";
            ret.ApiKeyGUID = null;
            ret.UserGUID = user.GUID;
            ret.Notes = "*** System generated ***";
            ret.ReadObject = true;
            ret.ReadContainer = true;
            ret.WriteObject = true;
            ret.WriteContainer = true;
            ret.DeleteObject = true;
            ret.DeleteContainer = true;
            ret.Active = true; 
            return ret;
        }
         
        internal Dictionary<string, object> ToInsertDictionary()
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("guid", GUID);
            ret.Add("containerguid", ContainerGUID);
            ret.Add("userguid", UserGUID);
            ret.Add("apikeyguid", ApiKeyGUID);
            ret.Add("notes", Notes);
            ret.Add("readobject", ReadObject);
            ret.Add("readcontainer", ReadContainer);
            ret.Add("writeobject", WriteObject);
            ret.Add("writecontainer", WriteContainer);
            ret.Add("deleteobject", DeleteObject);
            ret.Add("deletecontainer", DeleteContainer); 
            ret.Add("active", Active); 
            return ret;
        }
         
        internal static List<Column> GetTableColumns()
        {
            List<Column> ret = new List<Column>();
            ret.Add(new Column("id", true, DataType.Int, 11, null, false));
            ret.Add(new Column("guid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("containerguid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("userguid", false, DataType.Int, 11, null, true));
            ret.Add(new Column("apikeyguid", false, DataType.Int, 11, null, true));
            ret.Add(new Column("notes", false, DataType.Nvarchar, 256, null, true));
            ret.Add(new Column("readobject", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("readcontainer", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("writeobject", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("writecontainer", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("deleteobject", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("deletecontainer", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("active", false, DataType.Nvarchar, 8, null, false)); 
            return ret;
        } 
    }
}
