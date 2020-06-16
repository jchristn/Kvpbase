using System; 
using System.Collections.Generic;
using Watson.ORM;
using Watson.ORM.Core;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// Container key-value pair for metadata.
    /// </summary>
    [Table("containerkvps")]
    public class ContainerKeyValuePair
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
        public ContainerKeyValuePair()
        {
            GUID = Guid.NewGuid().ToString();
        }
         
        internal ContainerKeyValuePair(string containerGuid, string key, string val)
        {
            if (String.IsNullOrEmpty(containerGuid)) throw new ArgumentNullException(nameof(containerGuid));
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            GUID = Guid.NewGuid().ToString();
            ContainerGUID = containerGuid;
            MetadataKey = key;
            MetadataValue = val;
        } 
    }
}
