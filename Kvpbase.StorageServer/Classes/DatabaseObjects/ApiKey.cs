using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Watson.ORM;
using Watson.ORM.Core;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// API key for use when accessing the RESTful HTTP API.
    /// </summary>
    [Table("apikeys")]
    public class ApiKey
    { 
        /// <summary>
        /// Row ID in the database.
        /// </summary>
        [Column("id", true, DataTypes.Int, false)]
        public int Id { get; set; }

        /// <summary>
        /// Object GUID.  This value is used as the actual API key.
        /// </summary>
        [Column("guid", false, DataTypes.Nvarchar, 64, false)]
        public string GUID { get; set; }

        /// <summary>
        /// User GUID.
        /// </summary>
        [Column("userguid", false, DataTypes.Nvarchar, 64, false)]
        public string UserGUID { get; set; }

        /// <summary>
        /// Indicates if the account is active or disabled.
        /// </summary>
        [Column("active", false, DataTypes.Boolean, false)]
        public bool Active { get; set; }
          
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public ApiKey()
        {
            GUID = Guid.NewGuid().ToString();
        } 
    }
}
