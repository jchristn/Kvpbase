using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

using DatabaseWrapper;

namespace Kvpbase.Classes
{
    /// <summary>
    /// Permissions associated with a user or API key.
    /// </summary>
    public class Permission
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
        /// ID of the associated user.
        /// </summary>
        public int? UserMasterId { get; set; }

        /// <summary>
        /// ID of the associated API key.
        /// </summary>
        public int? ApiKeyId { get; set; }

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
        /// The timestamp from when the object was created.
        /// </summary>
        public DateTime CreatedUtc { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public Permission()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }

        /// <summary>
        /// Instantiate the object from a DataRow.
        /// </summary>
        /// <param name="row">DataRow.</param>
        /// <returns>Permission.</returns>
        public static Permission FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            Permission ret = new Permission();

            if (row.Table.Columns.Contains("Id") && row["Id"] != null && row["Id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["Id"]);

            if (row.Table.Columns.Contains("GUID") && row["GUID"] != null && row["GUID"] != DBNull.Value)
                ret.GUID = row["GUID"].ToString();

            if (row.Table.Columns.Contains("UserMasterId") && row["UserMasterId"] != null && row["UserMasterId"] != DBNull.Value)
                ret.UserMasterId = Convert.ToInt32(row["UserMasterId"]);

            if (row.Table.Columns.Contains("ApiKeyId") && row["ApiKeyId"] != null && row["ApiKeyId"] != DBNull.Value)
                ret.ApiKeyId = Convert.ToInt32(row["ApiKeyId"]);

            if (row.Table.Columns.Contains("Notes") && row["Notes"] != null && row["Notes"] != DBNull.Value)
                ret.Notes = row["Notes"].ToString();
             
            if (row.Table.Columns.Contains("ReadObject") && row["ReadObject"] != null && row["ReadObject"] != DBNull.Value)
                ret.ReadObject = Convert.ToBoolean(row["ReadObject"]);

            if (row.Table.Columns.Contains("ReadContainer") && row["ReadContainer"] != null && row["ReadContainer"] != DBNull.Value)
                ret.ReadContainer = Convert.ToBoolean(row["ReadContainer"]);

            if (row.Table.Columns.Contains("WriteObject") && row["WriteObject"] != null && row["WriteObject"] != DBNull.Value)
                ret.WriteObject = Convert.ToBoolean(row["WriteObject"]);

            if (row.Table.Columns.Contains("WriteContainer") && row["WriteContainer"] != null && row["WriteContainer"] != DBNull.Value)
                ret.WriteContainer = Convert.ToBoolean(row["WriteContainer"]);

            if (row.Table.Columns.Contains("DeleteObject") && row["DeleteObject"] != null && row["DeleteObject"] != DBNull.Value)
                ret.DeleteObject = Convert.ToBoolean(row["DeleteObject"]);

            if (row.Table.Columns.Contains("DeleteContainer") && row["DeleteContainer"] != null && row["DeleteContainer"] != DBNull.Value)
                ret.DeleteContainer = Convert.ToBoolean(row["DeleteContainer"]);

            if (row.Table.Columns.Contains("Active") && row["Active"] != null && row["Active"] != DBNull.Value)
                ret.Active = Convert.ToBoolean(row["Active"]);

            if (row.Table.Columns.Contains("CreatedUtc") && row["CreatedUtc"] != null && row["CreatedUtc"] != DBNull.Value)
                ret.CreatedUtc = Convert.ToDateTime(row["CreatedUtc"]);

            return ret;
        }

        public static Permission DefaultPermit(UserMaster user)
        {
            Permission ret = new Permission();
            ret.Id = 0;
            ret.GUID = Guid.NewGuid().ToString();
            ret.ApiKeyId = null;
            ret.UserMasterId = user.Id;
            ret.Notes = "*** System generated ***";
            ret.ReadObject = true;
            ret.ReadContainer = true;
            ret.WriteObject = true;
            ret.WriteContainer = true;
            ret.DeleteObject = true;
            ret.DeleteContainer = true;
            ret.Active = true;
            ret.CreatedUtc = DateTime.Now.ToUniversalTime();
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
            ret.Add("UserMasterId", UserMasterId);
            ret.Add("ApiKeyId", ApiKeyId);
            ret.Add("Notes", Notes);
            ret.Add("ReadObject", ReadObject);
            ret.Add("ReadContainer", ReadContainer);
            ret.Add("WriteObject", WriteObject);
            ret.Add("WriteContainer", WriteContainer);
            ret.Add("DeleteObject", DeleteObject);
            ret.Add("DeleteContainer", DeleteContainer); 
            ret.Add("Active", Active);
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
            ret.Add(new Column("UserMasterId", false, DataType.Int, 11, null, true));
            ret.Add(new Column("ApiKeyId", false, DataType.Int, 11, null, true));
            ret.Add(new Column("Notes", false, DataType.Nvarchar, 256, null, true));
            ret.Add(new Column("ReadObject", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("ReadContainer", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("WriteObject", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("WriteContainer", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("DeleteObject", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("DeleteContainer", false, DataType.Nvarchar, 8, null, true));
            ret.Add(new Column("Active", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("CreatedUtc", false, DataType.DateTime, 32, null, false));
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
