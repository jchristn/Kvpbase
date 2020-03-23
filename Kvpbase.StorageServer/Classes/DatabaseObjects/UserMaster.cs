using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;

using DatabaseWrapper;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// A Kvpbase user.
    /// </summary>
    public class UserMaster
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
        /// The first name.
        /// </summary>
        public string FirstName { get; set; }

        /// <summary>
        /// The last name.
        /// </summary>
        public string LastName { get; set; }

        /// <summary>
        /// The company name.
        /// </summary>
        public string CompanyName { get; set; }

        /// <summary>
        /// Email address.
        /// </summary>
        public string Email { get; set; }

        /// <summary>
        /// Password.
        /// </summary>
        public string Password { get; set; }
         
        /// <summary>
        /// The user's home directory.  If null, a directory will be created under the default storage directory.
        /// </summary>
        public string HomeDirectory { get; set; }
           
        /// <summary>
        /// Indicates if the object is active or disabled.
        /// </summary>
        public bool Active { get; set; }

        /// <summary>
        /// The timestamp from when the object was created.
        /// </summary>
        public DateTime CreatedUtc { get; set; }
         
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public UserMaster()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime(); 
        }
         
        internal static UserMaster FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            UserMaster ret = new UserMaster();

            if (row.Table.Columns.Contains("id") && row["id"] != null && row["id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["id"]);

            if (row.Table.Columns.Contains("guid") && row["guid"] != null && row["guid"] != DBNull.Value)
                ret.GUID = row["guid"].ToString();

            if (row.Table.Columns.Contains("firstname") && row["firstname"] != null && row["firstname"] != DBNull.Value)
                ret.FirstName = row["firstname"].ToString();

            if (row.Table.Columns.Contains("lastname") && row["lastname"] != null && row["lastname"] != DBNull.Value)
                ret.LastName = row["lastname"].ToString();

            if (row.Table.Columns.Contains("companyname") && row["companyname"] != null && row["companyname"] != DBNull.Value)
                ret.CompanyName = row["companyname"].ToString();

            if (row.Table.Columns.Contains("email") && row["email"] != null && row["email"] != DBNull.Value)
                ret.Email = row["email"].ToString();

            if (row.Table.Columns.Contains("password") && row["password"] != null && row["password"] != DBNull.Value)
                ret.Password = row["password"].ToString();
             
            if (row.Table.Columns.Contains("homedirectory") && row["homedirectory"] != null && row["homedirectory"] != DBNull.Value)
                ret.HomeDirectory = row["homedirectory"].ToString();

            if (row.Table.Columns.Contains("active") && row["active"] != null && row["active"] != DBNull.Value)
                ret.Active = Convert.ToBoolean(row["active"]);

            if (row.Table.Columns.Contains("createdutc") && row["createdutc"] != null && row["createdutc"] != DBNull.Value)
                ret.CreatedUtc = Convert.ToDateTime(row["createdutc"]);

            return ret;
        }

        internal static List<UserMaster> FromDataTable(DataTable table)
        {
            if (table == null) return null;
            List<UserMaster> ret = new List<UserMaster>();
            foreach (DataRow row in table.Rows) ret.Add(FromDataRow(row));
            return ret;
        }

        internal Dictionary<string, object> ToInsertDictionary()
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("guid", GUID);
            ret.Add("firstname", FirstName);
            ret.Add("lastname", LastName);
            ret.Add("companyname", CompanyName);
            ret.Add("email", Email);
            ret.Add("password", Password);
            ret.Add("homedirectory", HomeDirectory);
            ret.Add("active", Active);
            ret.Add("createdutc", CreatedUtc); 
            return ret;
        }
         
        internal static List<Column> GetTableColumns()
        { 
            List<Column> ret = new List<Column>();
            ret.Add(new Column("id", true, DataType.Int, 11, null, false));
            ret.Add(new Column("guid", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("firstname", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("lastname", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("companyname", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("email", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("password", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("homedirectory", false, DataType.Nvarchar, 256, null, true));
            ret.Add(new Column("active", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("createdutc", false, DataType.DateTime, 32, null, false));
            return ret;
        } 
    }
}
