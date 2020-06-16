using System; 
using System.Collections.Generic;
using Watson.ORM;
using Watson.ORM.Core;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// Object key-value pair for metadata.
    /// </summary>
    [Table("objectkvps")]
    public class ObjectKeyValuePair
    {
        /// <summary>
        /// Row ID in the database.
        /// </summary>
        [Column("id", true, DataTypes.Int, false)]
        public int Id { get; set; }

        /// <summary>
        /// GUID.
        /// </summary>
        [Column("guid", false, DataTypes.Nvarchar, 64, false)]
        public string GUID { get; set; }

        /// <summary>
        /// Container GUID.
        /// </summary>
        [Column("containerguid", false, DataTypes.Nvarchar, 64, false)]
        public string ContainerGUID { get; set; }

        /// <summary>
        /// Object GUID.
        /// </summary>
        [Column("objectguid", false, DataTypes.Nvarchar, 64, false)]
        public string ObjectGUID { get; set; }

        /// <summary>
        /// Metadata key.
        /// </summary>
        [Column("metadatakey", false, DataTypes.Nvarchar, 64, false)]
        public string MetadataKey { get; set; }

        /// <summary>
        /// Metadata value.
        /// </summary>
        [Column("metadatavalue", false, DataTypes.Nvarchar, 1024, true)]
        public string MetadataValue { get; set; } 
          
        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public ObjectKeyValuePair()
        {
            GUID = Guid.NewGuid().ToString();
        }
         
        internal ObjectKeyValuePair(string containerGuid, string objectGuid, string key, string val)
        {
            if (String.IsNullOrEmpty(containerGuid)) throw new ArgumentNullException(nameof(containerGuid));
            if (String.IsNullOrEmpty(objectGuid)) throw new ArgumentNullException(nameof(objectGuid));
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            GUID = Guid.NewGuid().ToString();
            ContainerGUID = containerGuid;
            ObjectGUID = objectGuid;
            MetadataKey = key;
            MetadataValue = val;
        } 
    }
}
