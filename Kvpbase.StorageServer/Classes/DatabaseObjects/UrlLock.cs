using System;
using System.Collections.Generic; 
using System.Net;
using Watson.ORM;
using Watson.ORM.Core;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// A lock applied to a URL for an in-progress operation.
    /// </summary>
    [Table("urllocks")]
    public class UrlLock
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
        /// The lock type.
        /// </summary>
        [Column("locktype", false, DataTypes.Enum, 8, false)]
        public LockType LockType { get; set; }

        /// <summary>
        /// The raw URL that is locked.
        /// </summary>
        [Column("url", false, DataTypes.Nvarchar, 512, false)]
        public string Url { get; set; }

        /// <summary>
        /// The GUID of the user that holds the lock.
        /// </summary>
        [Column("userguid", false, DataTypes.Nvarchar, 64, false)]
        public string UserGUID { get; set; }

        /// <summary>
        /// The hostname upon which the lock was generated.
        /// </summary>
        [Column("hostname", false, DataTypes.Nvarchar, 256, false)]
        public string Hostname { get; set; }

        /// <summary>
        /// The timestamp from when the object was created.
        /// </summary>
        [Column("createdutc", false, DataTypes.DateTime, false)]
        public DateTime CreatedUtc { get; set; }
         
        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public UrlLock()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime(); 
        }
         
        internal UrlLock(LockType lockType, string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url)); 

            GUID = Guid.NewGuid().ToString();
            LockType = lockType;
            Url = url;
            UserGUID = userGuid; 
            Hostname = Dns.GetHostName();
            CreatedUtc = DateTime.Now.ToUniversalTime();
        } 
    }
}
