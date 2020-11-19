using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Watson.ORM;
using Watson.ORM.Core;
using Newtonsoft.Json;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// Details about an audit log entry.
    /// </summary>
    [Table("auditlogentries")]
    public class AuditLogEntry
    {
        /// <summary>
        /// Row ID in the database.
        /// </summary>
        [Column("id", true, DataTypes.Int, false)]
        public int Id { get; set; }

        /// <summary>
        /// GUID.
        /// </summary>
        [JsonProperty(Order = -3)]
        [Column("guid", false, DataTypes.Nvarchar, 64, false)]
        public string GUID { get; set; }

        /// <summary>
        /// Container GUID.
        /// </summary>
        [JsonProperty(Order = -2)]
        [Column("containerguid", false, DataTypes.Nvarchar, 64, false)]
        public string ContainerGUID { get; set; }

        /// <summary>
        /// Object GUID.
        /// </summary>
        [JsonProperty(Order = -1)]
        [Column("objectguid", false, DataTypes.Nvarchar, 64, false)]
        public string ObjectGUID { get; set; }

        /// <summary>
        /// Action performed by the requestor.
        /// </summary>
        [Column("action", false, DataTypes.Enum, 32, false)]
        public AuditLogEntryType Action { get; set; }

        /// <summary>
        /// Metadata associated with the action.
        /// </summary>
        [Column("metadata", false, DataTypes.Nvarchar, 256, true)]
        public string Metadata { get; set; }

        /// <summary>
        /// Timestamp of the action.
        /// </summary>
        [JsonProperty(Order = 990)]
        [Column("createdutc", false, DataTypes.DateTime, false)]
        public DateTime CreatedUtc { get; set; }
         
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public AuditLogEntry()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }
         
        internal AuditLogEntry(string containerGuid, string objectGuid, AuditLogEntryType action, string metadata)
        {
            GUID = Guid.NewGuid().ToString();
            ContainerGUID = containerGuid; 
            ObjectGUID = objectGuid; 
            CreatedUtc = DateTime.Now.ToUniversalTime(); 
            Action = action;
            Metadata = metadata;
        } 
    }
}
