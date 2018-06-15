using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kvpbase
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
        /// The object's key.
        /// </summary>
        public string Key { get; set; }

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
            Initialize();
        }

        /// <summary>
        /// Instantiate the object from a DataRow.
        /// </summary>
        /// <param name="row">DataRow.</param>
        public ObjectMetadata(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            Initialize();

            if (row["Id"] != null && row["Id"] != DBNull.Value)
                Id = Convert.ToInt64(row["Id"]);

            if (row["Key"] != null && row["Key"] != DBNull.Value)
                Key = row["Key"].ToString();

            if (row["ContentType"] != null && row["ContentType"] != DBNull.Value)
                ContentType = row["ContentType"].ToString();

            if (row["ContentLength"] != null && row["ContentLength"] != DBNull.Value)
                ContentLength = Convert.ToInt64(row["ContentLength"]);

            if (row["Md5"] != null && row["Md5"] != DBNull.Value)
                Md5 = row["Md5"].ToString();

            if (row["CreatedUtc"] != null && row["CreatedUtc"] != DBNull.Value)
                CreatedUtc = Convert.ToDateTime(row["CreatedUtc"]);

            if (row["LastUpdateUtc"] != null && row["LastUpdateUtc"] != DBNull.Value)
                LastUpdateUtc = Convert.ToDateTime(row["LastUpdateUtc"]);

            if (row["LastAccessUtc"] != null && row["LastAccessUtc"] != DBNull.Value)
                LastAccessUtc = Convert.ToDateTime(row["LastAccessUtc"]);
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="key">The object's key.</param>
        /// <param name="contentType">The content type of the object.</param>
        /// <param name="data">The object's data.</param>
        public ObjectMetadata(string key, string contentType, byte[] data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(contentType)) contentType = "application/octet-stream";
            if (data == null) data = new byte[0];

            Initialize();
            Key = key;
            ContentType = contentType;
            ContentLength = data.Length;

            if (data != null && data.Length > 0) Md5 = Common.Md5(data);
            else Md5 = null;

            DateTime ts = DateTime.Now.ToUniversalTime();
            CreatedUtc = ts;
            LastUpdateUtc = ts;
            LastAccessUtc = ts;
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
                ret.Add(new ObjectMetadata(curr));
            }

            return ret;
        }

        #endregion

        #region Private-Methods

        private void Initialize()
        {
            Id = 0;
            Key = null;
            ContentType = "application/octet-stream";
            ContentLength = 0;
            Md5 = null;
            CreatedUtc = null;
            LastUpdateUtc = null;
            LastAccessUtc = null;
        }

        #endregion
    }
}
