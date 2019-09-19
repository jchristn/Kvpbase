using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;

using DatabaseWrapper;

namespace Kvpbase.Classes
{
    /// <summary>
    /// A Kvpbase user.
    /// </summary>
    public class UserMaster
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
        /// Cellular phone number.
        /// </summary>
        public string Cellphone { get; set; }

        /// <summary>
        /// Address line 1.
        /// </summary>
        public string Address1 { get; set; }

        /// <summary>
        /// Address line 2.
        /// </summary>
        public string Address2 { get; set; }

        /// <summary>
        /// City.
        /// </summary>
        public string City { get; set; }

        /// <summary>
        /// State.
        /// </summary>
        public string State { get; set; }

        /// <summary>
        /// Postal code.
        /// </summary>
        public string PostalCode { get; set; }

        /// <summary>
        /// Country (recommend using the ISO A3 country code).
        /// </summary>
        public string Country { get; set; }
         
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

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public UserMaster()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime(); 
        }

        /// <summary>
        /// Instantiate the object from a DataRow.
        /// </summary>
        /// <param name="row">DataRow.</param>
        /// <returns>UserMaster.</returns>
        public static UserMaster FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            UserMaster ret = new UserMaster();

            if (row.Table.Columns.Contains("Id") && row["Id"] != null && row["Id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["Id"]);

            if (row.Table.Columns.Contains("GUID") && row["GUID"] != null && row["GUID"] != DBNull.Value)
                ret.GUID = row["GUID"].ToString();

            if (row.Table.Columns.Contains("FirstName") && row["FirstName"] != null && row["FirstName"] != DBNull.Value)
                ret.FirstName = row["FirstName"].ToString();

            if (row.Table.Columns.Contains("LastName") && row["LastName"] != null && row["LastName"] != DBNull.Value)
                ret.LastName = row["LastName"].ToString();

            if (row.Table.Columns.Contains("CompanyName") && row["CompanyName"] != null && row["CompanyName"] != DBNull.Value)
                ret.CompanyName = row["CompanyName"].ToString();

            if (row.Table.Columns.Contains("Email") && row["Email"] != null && row["Email"] != DBNull.Value)
                ret.Email = row["Email"].ToString();

            if (row.Table.Columns.Contains("Password") && row["Password"] != null && row["Password"] != DBNull.Value)
                ret.Password = row["Password"].ToString();

            if (row.Table.Columns.Contains("Cellphone") && row["Cellphone"] != null && row["Cellphone"] != DBNull.Value)
                ret.Cellphone = row["Cellphone"].ToString();

            if (row.Table.Columns.Contains("Address1") && row["Address1"] != null && row["Address1"] != DBNull.Value)
                ret.Address1 = row["Address1"].ToString();

            if (row.Table.Columns.Contains("Address2") && row["Address2"] != null && row["Address2"] != DBNull.Value)
                ret.Address2 = row["Address2"].ToString();

            if (row.Table.Columns.Contains("City") && row["City"] != null && row["City"] != DBNull.Value)
                ret.City = row["City"].ToString();

            if (row.Table.Columns.Contains("State") && row["State"] != null && row["State"] != DBNull.Value)
                ret.State = row["State"].ToString();

            if (row.Table.Columns.Contains("PostalCode") && row["PostalCode"] != null && row["PostalCode"] != DBNull.Value)
                ret.PostalCode = row["PostalCode"].ToString();

            if (row.Table.Columns.Contains("Country") && row["Country"] != null && row["Country"] != DBNull.Value)
                ret.Country = row["Country"].ToString();

            if (row.Table.Columns.Contains("HomeDirectory") && row["HomeDirectory"] != null && row["HomeDirectory"] != DBNull.Value)
                ret.HomeDirectory = row["HomeDirectory"].ToString();

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
            ret.Add("FirstName", FirstName);
            ret.Add("LastName", LastName);
            ret.Add("CompanyName", CompanyName);
            ret.Add("Email", Email);
            ret.Add("Password", Password);
            ret.Add("Cellphone", Cellphone);
            ret.Add("Address1", Address1);
            ret.Add("Address2", Address2);
            ret.Add("City", City);
            ret.Add("State", State);
            ret.Add("PostalCode", PostalCode);
            ret.Add("Country", Country);
            ret.Add("HomeDirectory", HomeDirectory);
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
            ret.Add(new Column("FirstName", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("LastName", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("CompanyName", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("Email", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("Password", false, DataType.Nvarchar, 64, null, false));
            ret.Add(new Column("Cellphone", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("Address1", false, DataType.Nvarchar, 128, null, true));
            ret.Add(new Column("Address2", false, DataType.Nvarchar, 128, null, true));
            ret.Add(new Column("City", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("State", false, DataType.Nvarchar, 32, null, true));
            ret.Add(new Column("PostalCode", false, DataType.Nvarchar, 32, null, true));
            ret.Add(new Column("Country", false, DataType.Nvarchar, 32, null, true));
            ret.Add(new Column("HomeDirectory", false, DataType.Nvarchar, 256, null, true));
            ret.Add(new Column("Active", false, DataType.Nvarchar, 8, null, false));
            ret.Add(new Column("CreatedUtc", false, DataType.DateTime, 32, null, false));
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
