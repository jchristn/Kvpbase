using System;
using System.Collections.Generic;
using Watson.ORM;
using Watson.ORM.Core;
using Newtonsoft.Json;

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
        [JsonIgnore]
        [Column("id", true, DataTypes.Int, false)]
        public int Id { get; set; }

        /// <summary>
        /// GUID.
        /// </summary>
        [JsonProperty(Order = -4)]
        [Column("guid", false, DataTypes.Nvarchar, 64, false)]
        public string GUID { get; set; }

        /// <summary>
        /// Container GUID.
        /// </summary>
        [JsonProperty(Order = -3)]
        [Column("containerguid", false, DataTypes.Nvarchar, 64, false)]
        public string ContainerGUID { get; set; }

        /// <summary>
        /// GUID of the associated user.
        /// </summary>
        [JsonProperty(Order = -2)]
        [Column("userguid", false, DataTypes.Nvarchar, 64, false)]
        public string UserGUID { get; set; }

        /// <summary>
        /// GUID of the associated API key.
        /// </summary>
        [JsonProperty(Order = -1)]
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
        [JsonProperty(Order = 990)]
        [Column("readobject", false, DataTypes.Boolean, false)]
        public bool ReadObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to read containers.
        /// </summary>
        [JsonProperty(Order = 991)]
        [Column("readcontainer", false, DataTypes.Boolean, false)]
        public bool ReadContainer { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to write objects.
        /// </summary>
        [JsonProperty(Order = 992)]
        [Column("writeobject", false, DataTypes.Boolean, false)]
        public bool WriteObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to write containers.
        /// </summary>
        [JsonProperty(Order = 993)]
        [Column("writecontainer", false, DataTypes.Boolean, false)]
        public bool WriteContainer { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to delete objects.
        /// </summary>
        [JsonProperty(Order = 994)]
        [Column("deleteobject", false, DataTypes.Boolean, false)]
        public bool DeleteObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to delete containers.
        /// </summary>
        [JsonProperty(Order = 995)]
        [Column("deletecontainer", false, DataTypes.Boolean, false)]
        public bool DeleteContainer { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to execute administrative APIs.
        /// </summary>
        [JsonProperty(Order = 996)]
        [Column("isadmin", false, DataTypes.Boolean, false)]
        public bool IsAdmin { get; set; }

        /// <summary>
        /// Indicates if the object is active or disabled.
        /// </summary>
        [JsonProperty(Order = 997)]
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
