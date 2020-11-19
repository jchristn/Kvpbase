using System; 
using System.Collections.Generic;
using Watson.ORM;
using Watson.ORM.Core;
using Newtonsoft.Json;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// A Kvpbase user.
    /// </summary>
    [Table("users")]
    public class UserMaster
    {
        /// <summary>
        /// Row ID in the database.
        /// </summary>
        [JsonIgnore]
        [Column("id", true, DataTypes.Int, false)]
        public int Id { get; set; }

        /// <summary>
        /// GUID.
        /// </summary>
        [JsonProperty(Order = -6)]
        [Column("guid", false, DataTypes.Nvarchar, 64, false)]
        public string GUID { get; set; }

        /// <summary>
        /// The first name.
        /// </summary>
        [JsonProperty(Order = -5)]
        [Column("firstname", false, DataTypes.Nvarchar, 64, true)]
        public string FirstName { get; set; }

        /// <summary>
        /// The last name.
        /// </summary>
        [JsonProperty(Order = -4)]
        [Column("lastname", false, DataTypes.Nvarchar, 64, true)]
        public string LastName { get; set; }

        /// <summary>
        /// The company name.
        /// </summary>
        [JsonProperty(Order = -3)]
        [Column("companyname", false, DataTypes.Nvarchar, 64, true)]
        public string CompanyName { get; set; }

        /// <summary>
        /// Email address.
        /// </summary>
        [JsonProperty(Order = -2)]
        [Column("email", false, DataTypes.Nvarchar, 64, false)]
        public string Email { get; set; }

        /// <summary>
        /// Password.
        /// </summary>
        [JsonProperty(Order = -1)]
        [Column("password", false, DataTypes.Nvarchar, 64, true)]
        public string Password { get; set; }

        /// <summary>
        /// The user's home directory.  If null, a directory will be created under the default storage directory.
        /// </summary>
        [Column("homedirectory", false, DataTypes.Nvarchar, 256, true)]
        public string HomeDirectory { get; set; }

        /// <summary>
        /// Indicates if the object is active or disabled.
        /// </summary>
        [JsonProperty(Order = 990)]
        [Column("active", false, DataTypes.Boolean, false)]
        public bool Active { get; set; }

        /// <summary>
        /// The timestamp from when the object was created.
        /// </summary>
        [JsonProperty(Order = 991)]
        [Column("createdutc", false, DataTypes.DateTime, false)]
        public DateTime CreatedUtc { get; set; }
         
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public UserMaster()
        {
            GUID = Guid.NewGuid().ToString();
            CreatedUtc = DateTime.Now.ToUniversalTime(); 
        } 
    }
}
