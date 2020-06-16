using System;
using System.Collections.Generic;
using Watson.ORM;
using Watson.ORM.Core;

namespace Kvpbase.StorageServer.Classes.DatabaseObjects
{
    /// <summary>
    /// Permissions associated with a user or API key.
    /// </summary>
    [Table("permissions")]
    public class Permission
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
        /// GUID of the associated user.
        /// </summary>
        [Column("userguid", false, DataTypes.Nvarchar, 64, false)]
        public string UserGUID { get; set; }

        /// <summary>
        /// GUID of the associated API key.
        /// </summary>
        [Column("apikeyguid", false, DataTypes.Nvarchar, 64, false)]
        public string ApiKeyGUID { get; set; }

        /// <summary>
        /// Administrator notes for the API key.
        /// </summary>
        [Column("notes", false, DataTypes.Nvarchar, 256, true)]
        public string Notes { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to read objects.
        /// </summary>
        [Column("readobject", false, DataTypes.Boolean, false)]
        public bool ReadObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to read containers.
        /// </summary>
        [Column("readcontainer", false, DataTypes.Boolean, false)]
        public bool ReadContainer { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to write objects.
        /// </summary>
        [Column("writeobject", false, DataTypes.Boolean, false)]
        public bool WriteObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to write containers.
        /// </summary>
        [Column("writecontainer", false, DataTypes.Boolean, false)]
        public bool WriteContainer { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to delete objects.
        /// </summary>
        [Column("deleteobject", false, DataTypes.Boolean, false)]
        public bool DeleteObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to delete containers.
        /// </summary>
        [Column("deletecontainer", false, DataTypes.Boolean, false)]
        public bool DeleteContainer { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to execute administrative APIs.
        /// </summary>
        [Column("isadmin", false, DataTypes.Boolean, false)]
        public bool IsAdmin { get; set; }

        /// <summary>
        /// Indicates if the object is active or disabled.
        /// </summary>
        [Column("active", false, DataTypes.Boolean, false)]
        public bool Active { get; set; } 
         
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public Permission()
        { 
        }
          
        internal static Permission DefaultPermit(UserMaster user)
        {
            Permission ret = new Permission();
            ret.Id = 0;
            ret.GUID = Guid.NewGuid().ToString();
            ret.ContainerGUID = "*";
            ret.ApiKeyGUID = null;
            ret.UserGUID = user.GUID;
            ret.Notes = "*** System generated ***";
            ret.ReadObject = true;
            ret.ReadContainer = true;
            ret.WriteObject = true;
            ret.WriteContainer = true;
            ret.DeleteObject = true;
            ret.DeleteContainer = true;
            ret.IsAdmin = false;
            ret.Active = true; 
            return ret;
        } 
    }
}
