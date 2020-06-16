using System; 
using System.Collections.Generic;
using Watson.ORM;
using Watson.ORM.Core;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// Object container.
    /// </summary>
    [Table("containers")]
    public class Container
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
        /// The username of the owner.
        /// </summary>
        [Column("userguid", false, DataTypes.Nvarchar, 64, false)]
        public string UserGUID { get; set; }

        /// <summary>
        /// The name of the container.
        /// </summary>
        [Column("name", false, DataTypes.Nvarchar, 64, false)]
        public string Name { get; set; }

        /// <summary>
        /// The full path to where container objects should be stored.
        /// </summary>
        [Column("objectsdirectory", false, DataTypes.Nvarchar, 256, false)]
        public string ObjectsDirectory { get; set; }

        /// <summary>
        /// Enable or disable audit logging.
        /// </summary>
        [Column("enableauditlogging", false, DataTypes.Boolean, false)]
        public bool EnableAuditLogging { get; set; }

        /// <summary>
        /// Enable or disable public read access.
        /// </summary>
        [Column("ispublicread", false, DataTypes.Boolean, false)]
        public bool IsPublicRead { get; set; }

        /// <summary>
        /// Enable or disable public write access.
        /// </summary>
        [Column("ispublicwrite", false, DataTypes.Boolean, false)]
        public bool IsPublicWrite { get; set; }

        /// <summary>
        /// The timestamp from when the object was created.
        /// </summary>
        [Column("createdutc", false, DataTypes.DateTime, true)]
        public DateTime? CreatedUtc { get; set; }
         
        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public Container()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        } 
    }
}
