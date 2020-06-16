using System; 
using System.Collections.Generic;
using Watson.ORM;
using Watson.ORM.Core;

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
        [Column("id", true, DataTypes.Int, false)]
        public int Id { get; set; }

        /// <summary>
        /// GUID.
        /// </summary>
        [Column("guid", false, DataTypes.Nvarchar, 64, false)]
        public string GUID { get; set; }

        /// <summary>
        /// The first name.
        /// </summary>
        [Column("firstname", false, DataTypes.Nvarchar, 64, true)]
        public string FirstName { get; set; }

        /// <summary>
        /// The last name.
        /// </summary>
        [Column("lastname", false, DataTypes.Nvarchar, 64, true)]
        public string LastName { get; set; }

        /// <summary>
        /// The company name.
        /// </summary>
        [Column("companyname", false, DataTypes.Nvarchar, 64, true)]
        public string CompanyName { get; set; }

        /// <summary>
        /// Email address.
        /// </summary>
        [Column("email", false, DataTypes.Nvarchar, 64, false)]
        public string Email { get; set; }

        /// <summary>
        /// Password.
        /// </summary>
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
        [Column("active", false, DataTypes.Boolean, false)]
        public bool Active { get; set; }

        /// <summary>
        /// The timestamp from when the object was created.
        /// </summary>
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
