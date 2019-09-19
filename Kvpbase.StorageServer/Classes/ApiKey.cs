using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

using DatabaseWrapper;

namespace Kvpbase.Classes
{
    /// <summary>
    /// API key for use when accessing the RESTful HTTP API.
    /// </summary>
    public class ApiKey
    {
        #region Public-Members

        /// <summary>
        /// Row ID in the database.
        /// </summary>
        public int? Id { get; set; }

        /// <summary>
        /// Object GUID.  This value is used as the actual API key.
        /// </summary>
        public string GUID { get; set; }
         
        /// <summary>
        /// ID of the user to which the API key is mapped.
        /// </summary>
        public int? UserMasterId { get; set; }
         
        /// <summary>
        /// Administrator notes for the API key.
        /// </summary>
        public string Notes { get; set; }

        /// <summary>
        /// Indicates if the account is active or disabled.
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
        public ApiKey()
        {

        }

        /// <summary>
        /// Instantiate the object from a DataRow.
        /// </summary>
        /// <param name="row">DataRow.</param>
        /// <returns>ApiKey.</returns>
        public static ApiKey FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            ApiKey ret = new ApiKey();
             
            if (row.Table.Columns.Contains("Id") && row["Id"] != null && row["Id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["Id"]);

            if (row.Table.Columns.Contains("GUID") && row["GUID"] != null && row["GUID"] != DBNull.Value)
                ret.GUID = row["GUID"].ToString();

            if (row.Table.Columns.Contains("UserMasterId") && row["UserMasterId"] != null && row["UserMasterId"] != DBNull.Value)
                ret.UserMasterId = Convert.ToInt32(row["UserMasterId"]);

            if (row.Table.Columns.Contains("Notes") && row["Notes"] != null && row["Notes"] != DBNull.Value)
                ret.Notes = row["Notes"].ToString();
             
            if (row.Table.Columns.Contains("Active") && row["Active"] != null && row["Active"] != DBNull.Value)
                ret.Active = Convert.ToBoolean(row["Active"]);

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
            ret.Add("UserMasterId", UserMasterId);
            ret.Add("Notes", Notes); 
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
            ret.Add(new Column("UserMasterId", false, DataType.Int, 11, null, false));
            ret.Add(new Column("Notes", false, DataType.Nvarchar, 256, null, true));
            ret.Add(new Column("Active", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("CreatedUtc", false, DataType.DateTime, 32, null, false));
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
