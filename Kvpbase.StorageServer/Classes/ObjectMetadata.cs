using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DatabaseWrapper;

namespace Kvpbase.Classes
{
    /// <summary>
    /// Metadata describing an object.
    /// </summary>
    public class ObjectMetadata
    {
        #region Public-Members

        /// <summary>
        /// The ID of the object.
        /// </summary>
        public long? Id { get; set; }

        /// <summary>
        /// The GUID of the object, also used as a unique name to store the object.
        /// </summary>
        public string GUID { get; set; }

        /// <summary>
        /// The object's key.
        /// </summary>
        public string ObjectKey { get; set; }

        /// <summary>
        /// The content type of the object.
        /// </summary>
        public string ContentType { get; set; }

        /// <summary>
        /// The content length of the object.
        /// </summary>
        public long? ContentLength { get; set; }

        /// <summary>
        /// The MD5 hash of the object's data.
        /// </summary>
        public string Md5 { get; set; }

        /// <summary>
        /// The comma-separated list of tags associated with an object.
        /// </summary>
        public List<string> Tags { get; set; }

        /// <summary>
        /// The creation timestamp, in UTC.
        /// </summary>
        public DateTime? CreatedUtc { get; set; }

        /// <summary>
        /// The time of last update, in UTC.
        /// </summary>
        public DateTime? LastUpdateUtc { get; set; }

        /// <summary>
        /// The time of last access, in UTC.
        /// </summary>
        public DateTime? LastAccessUtc { get; set; }
         
        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public ObjectMetadata()
        {
            Id = 0;
            GUID = Guid.NewGuid().ToString();
            ObjectKey = null;
            ContentType = "application/octet-stream";
            ContentLength = 0;
            Md5 = null;
            CreatedUtc = null;
            LastUpdateUtc = null;
            LastAccessUtc = null;
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="contentType">The content type of the object.</param>
        /// <param name="data">The object's data.</param>
        /// <param name="tags">Tags associated with the object.</param>
        public ObjectMetadata(string key, string contentType, byte[] data, List<string> tags)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(contentType)) contentType = "application/octet-stream";
            if (data == null) data = new byte[0];

            ObjectKey = key;
            GUID = Guid.NewGuid().ToString();
            ContentType = contentType;
            ContentLength = data.Length;
            Tags = tags;

            if (data != null && data.Length > 0) Md5 = Common.Md5(data);
            else Md5 = null;
            
            DateTime ts = DateTime.Now.ToUniversalTime();
            CreatedUtc = ts;
            LastUpdateUtc = ts;
            LastAccessUtc = ts;
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="contentType">The content type of the object.</param>
        /// <param name="contentLength">The object's length.</param>
        /// <param name="tags">Tags associated with the object.</param>
        public ObjectMetadata(string key, string contentType, long contentLength, List<string> tags)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(contentType)) contentType = "application/octet-stream";
            if (contentLength < 0) throw new ArgumentException("Invalid content length.");

            ObjectKey = key;
            GUID = Guid.NewGuid().ToString();
            ContentType = contentType;
            ContentLength = contentLength;
            Tags = tags;
            Md5 = null;
            
            DateTime ts = DateTime.Now.ToUniversalTime();
            CreatedUtc = ts;
            LastUpdateUtc = ts;
            LastAccessUtc = ts;
        }

        /// <summary>
        /// Instantiate the object from a DataRow.
        /// </summary>
        /// <param name="row">DataRow.</param>
        /// <returns>ObjectMetadata.</returns>
        public static ObjectMetadata FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            ObjectMetadata ret = new ObjectMetadata();

            if (row["Id"] != null && row["Id"] != DBNull.Value)
                ret.Id = Convert.ToInt64(row["Id"]);

            if (row["ObjectKey"] != null && row["ObjectKey"] != DBNull.Value)
                ret.ObjectKey = row["ObjectKey"].ToString();

            if (row["GUID"] != null && row["GUID"] != DBNull.Value)
                ret.GUID = row["GUID"].ToString();

            if (row["ContentType"] != null && row["ContentType"] != DBNull.Value)
                ret.ContentType = row["ContentType"].ToString();

            if (row["ContentLength"] != null && row["ContentLength"] != DBNull.Value)
                ret.ContentLength = Convert.ToInt64(row["ContentLength"]);

            if (row["Md5"] != null && row["Md5"] != DBNull.Value)
                ret.Md5 = row["Md5"].ToString();

            if (row["Tags"] != null && row["Tags"] != DBNull.Value) 
                ret.Tags = Common.CsvToStringList(row["Tags"].ToString());

            if (row["CreatedUtc"] != null && row["CreatedUtc"] != DBNull.Value)
                ret.CreatedUtc = Convert.ToDateTime(row["CreatedUtc"]);

            if (row["LastUpdateUtc"] != null && row["LastUpdateUtc"] != DBNull.Value)
                ret.LastUpdateUtc = Convert.ToDateTime(row["LastUpdateUtc"]);

            if (row["LastAccessUtc"] != null && row["LastAccessUtc"] != DBNull.Value)
                ret.LastAccessUtc = Convert.ToDateTime(row["LastAccessUtc"]);

            return ret;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Retrieve list of object metadata from a DataTable.
        /// </summary>
        /// <param name="dt">DataTable.</param>
        /// <returns>List of ObjectMetadata.</returns>
        public static List<ObjectMetadata> FromDataTable(DataTable dt)
        {
            if (dt == null) return null;
            if (dt.Rows.Count < 1) return new List<ObjectMetadata>();

            List<ObjectMetadata> ret = new List<ObjectMetadata>();

            foreach (DataRow curr in dt.Rows)
            {
                ret.Add(ObjectMetadata.FromDataRow(curr));
            }

            return ret;
        }

        /// <summary>
        /// Create a Dictionary from the object.
        /// </summary>
        /// <returns>Dictionary.</returns>
        public Dictionary<string, object> ToInsertDictionary()
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("GUID", GUID);
            ret.Add("ObjectKey", ObjectKey);
            ret.Add("ContentType", ContentType);
            ret.Add("ContentLength", ContentLength);
            ret.Add("Md5", Md5);
            ret.Add("Tags", Common.StringListToCsv(Tags));
            ret.Add("CreatedUtc", CreatedUtc);
            ret.Add("LastUpdateUtc", LastUpdateUtc);
            ret.Add("LastAccessUtc", LastAccessUtc); 
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
            ret.Add(new Column("ObjectKey", false, DataType.Nvarchar, 256, null, false));
            ret.Add(new Column("ContentType", false, DataType.Nvarchar, 256, null, true));
            ret.Add(new Column("ContentLength", false, DataType.Long, 12, null, false));
            ret.Add(new Column("Md5", false, DataType.Nvarchar, 64, null, true));
            ret.Add(new Column("Tags", false, DataType.Nvarchar, 1024, null, true));
            ret.Add(new Column("CreatedUtc", false, DataType.DateTime, 32, null, false));
            ret.Add(new Column("LastUpdateUtc", false, DataType.DateTime, 32, null, false));
            ret.Add(new Column("LastAccessUtc", false, DataType.DateTime, 32, null, false)); 
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
