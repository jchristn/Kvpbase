using System;
using System.Collections.Generic;
using System.Data;
using Watson.ORM;
using Watson.ORM.Core;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// Metadata describing an object.
    /// </summary>
    [Table("objects")]
    public class ObjectMetadata
    {
        /// <summary>
        /// The ID of the object.
        /// </summary>
        [Column("id", true, DataTypes.Int, false)]
        public int Id { get; set; }

        /// <summary>
        /// The GUID of the object, also used as a unique name to store the object.
        /// </summary>
        [Column("guid", false, DataTypes.Nvarchar, 64, false)]
        public string GUID { get; set; }

        /// <summary>
        /// The GUID of the container that contains the object. 
        /// </summary>
        [Column("containerguid", false, DataTypes.Nvarchar, 64, false)]
        public string ContainerGUID { get; set; }

        /// <summary>
        /// The object's key.
        /// </summary>
        [Column("objectkey", false, DataTypes.Nvarchar, 256, false)]
        public string ObjectKey { get; set; }

        /// <summary>
        /// The content type of the object.
        /// </summary>
        [Column("contenttype", false, DataTypes.Nvarchar, 256, true)]
        public string ContentType { get; set; }

        /// <summary>
        /// The content length of the object.
        /// </summary>
        [Column("contentlength", false, DataTypes.Long, false)]
        public long ContentLength { get; set; }

        /// <summary>
        /// The MD5 hash of the object's data.
        /// </summary>
        [Column("md5", false, DataTypes.Nvarchar, 64, true)]
        public string Md5 { get; set; }

        /// <summary>
        /// The comma-separated list of tags associated with an object.
        /// </summary>
        [Column("tags", false, DataTypes.Nvarchar, 1024, true)]
        public string Tags { get; set; }

        /// <summary>
        /// The creation timestamp, in UTC.
        /// </summary>
        [Column("createdutc", false, DataTypes.DateTime, false)]
        public DateTime? CreatedUtc { get; set; }

        /// <summary>
        /// The time of last update, in UTC.
        /// </summary>
        [Column("lastupdateutc", false, DataTypes.DateTime, false)]
        public DateTime? LastUpdateUtc { get; set; }

        /// <summary>
        /// The time of last access, in UTC.
        /// </summary>
        [Column("lastaccessutc", false, DataTypes.DateTime, false)]
        public DateTime? LastAccessUtc { get; set; }
          
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
         
        internal ObjectMetadata(string containerGuid, string key, string contentType, byte[] data, string tags)
        {
            if (String.IsNullOrEmpty(containerGuid)) throw new ArgumentNullException(nameof(containerGuid));
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(contentType)) contentType = "application/octet-stream";
            if (data == null) data = new byte[0];

            ContainerGUID = containerGuid;
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
         
        internal ObjectMetadata(string containerGuid, string key, string contentType, long contentLength, string tags)
        {
            if (String.IsNullOrEmpty(containerGuid)) throw new ArgumentNullException(nameof(containerGuid));
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(contentType)) contentType = "application/octet-stream";
            if (contentLength < 0) throw new ArgumentException("Invalid content length.");

            ContainerGUID = containerGuid;
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

        internal static ObjectMetadata FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            ObjectMetadata ret = new ObjectMetadata();

            if (row.Table.Columns.Contains("id") && row["id"] != null && row["id"] != DBNull.Value)
                ret.Id = Convert.ToInt32(row["id"]);

            if (row.Table.Columns.Contains("guid") && row["guid"] != null && row["guid"] != DBNull.Value)
                ret.GUID = row["guid"].ToString();

            if (row.Table.Columns.Contains("containerguid") && row["containerguid"] != null && row["containerguid"] != DBNull.Value)
                ret.ContainerGUID = row["containerguid"].ToString();

            if (row.Table.Columns.Contains("objectkey") && row["objectkey"] != null && row["objectkey"] != DBNull.Value)
                ret.ObjectKey = row["objectkey"].ToString();

            if (row.Table.Columns.Contains("contenttype") && row["contenttype"] != null && row["contenttype"] != DBNull.Value)
                ret.ContentType = row["contenttype"].ToString();

            if (row.Table.Columns.Contains("contentlength") && row["contentlength"] != null && row["contentlength"] != DBNull.Value)
                ret.ContentLength = Convert.ToInt64(row["contentlength"]);

            if (row.Table.Columns.Contains("md5") && row["md5"] != null && row["md5"] != DBNull.Value)
                ret.Md5 = row["md5"].ToString();

            if (row.Table.Columns.Contains("tags") && row["tags"] != null && row["tags"] != DBNull.Value)
                ret.Tags = row["tags"].ToString();

            if (row.Table.Columns.Contains("createdutc") && row["createdutc"] != null && row["createdutc"] != DBNull.Value)
                ret.CreatedUtc = Convert.ToDateTime(row["createdutc"]);

            if (row.Table.Columns.Contains("lastupdateutc") && row["lastupdateutc"] != null && row["lastupdateutc"] != DBNull.Value)
                ret.LastUpdateUtc = Convert.ToDateTime(row["lastupdateutc"]);

            if (row.Table.Columns.Contains("lastaccessutc") && row["lastaccessutc"] != null && row["lastaccessutc"] != DBNull.Value)
                ret.LastAccessUtc = Convert.ToDateTime(row["lastaccessutc"]);

            return ret;
        }

        internal static List<ObjectMetadata> FromDataTable(DataTable table)
        {
            if (table == null) return null;
            List<ObjectMetadata> ret = new List<ObjectMetadata>();
            foreach (DataRow row in table.Rows) ret.Add(FromDataRow(row));
            return ret;
        }
    }
}
