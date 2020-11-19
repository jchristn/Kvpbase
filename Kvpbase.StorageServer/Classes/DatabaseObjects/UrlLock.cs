using System;
using System.Collections.Generic; 
using System.Net;
using Watson.ORM;
using Watson.ORM.Core;
using Newtonsoft.Json;

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
        [JsonProperty(Order = -4)]
        [Column("guid", false, DataTypes.Nvarchar, 64, false)]
        public string GUID { get; set; }

        /// <summary>
        /// The lock type.
        /// </summary>
        [JsonProperty(Order = -3)]
        [Column("locktype", false, DataTypes.Nvarchar, 8, false)]
        public LockType LockType { get; set; }

        /// <summary>
        /// The raw URL that is locked.
        /// </summary>
        [JsonProperty(Order = -2)]
        [Column("url", false, DataTypes.Nvarchar, 512, false)]
        public string Url { get; set; }

        /// <summary>
        /// The GUID of the user that holds the lock.
        /// </summary>
        [JsonProperty(Order = -1)]
        [Column("userguid", false, DataTypes.Nvarchar, 64, true)]
        public string UserGUID { get; set; }

        /// <summary>
        /// The hostname upon which the lock was generated.
        /// </summary>
        [Column("hostname", false, DataTypes.Nvarchar, 256, false)]
        public string Hostname { get; set; }

        /// <summary>
        /// The timestamp from when the object was created.
        /// </summary>
        [JsonProperty(Order = 990)]
        [Column("createdutc", false, DataTypes.DateTime, false)]
        public DateTime CreatedUtc { get; set; }

        /// <summary>
        /// The timestamp for when the lock should expire.
        /// </summary>
        [JsonProperty(Order = 991)]
        [Column("expirationutc", false, DataTypes.DateTime, false)]
        public DateTime ExpirationUtc { get; set; }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public UrlLock()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime(); 
        }
         
        internal UrlLock(LockType lockType, string url, string userGuid, DateTime expirationUtc)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            if (!Common.IsLaterThanNow(expirationUtc)) throw new ArgumentException("Expiration timestamp must be later than the curren time.");

            GUID = Guid.NewGuid().ToString();
            LockType = lockType;
            Url = url;
            UserGUID = userGuid; 
            Hostname = Dns.GetHostName();
            CreatedUtc = DateTime.Now.ToUniversalTime();
            ExpirationUtc = expirationUtc.ToUniversalTime();
        } 
    }
}
