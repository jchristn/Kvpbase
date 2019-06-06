using System;
using System.Collections.Generic;
using System.IO; 

namespace Kvpbase.Core
{
    /// <summary>
    /// Permissions associated with an API key.
    /// </summary>
    public class ApiKeyPermission
    {
        #region Public-Members

        /// <summary>
        /// ID of the permissions.
        /// </summary>
        public int? ApiKeyPermissionId { get; set; }

        /// <summary>
        /// ID of the associated user.
        /// </summary>
        public int? UserMasterId { get; set; }

        /// <summary>
        /// ID of the associated API key.
        /// </summary>
        public int? ApiKeyId { get; set; }

        /// <summary>
        /// Administrator notes for the API key.
        /// </summary>
        public string Notes { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to read objects.
        /// </summary>
        public bool ReadObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to read containers.
        /// </summary>
        public bool ReadContainer { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to write objects.
        /// </summary>
        public bool WriteObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to write containers.
        /// </summary>
        public bool WriteContainer { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to delete objects.
        /// </summary>
        public bool DeleteObject { get; set; }

        /// <summary>
        /// Indicates whether or not the user has permission to delete containers.
        /// </summary>
        public bool DeleteContainer { get; set; } 

        /// <summary>
        /// GUID for the permission object.
        /// </summary>
        public string Guid { get; set; }

        /// <summary>
        /// Indicates whether or not the API key is active.
        /// </summary>
        public bool Active { get; set; }

        /// <summary>
        /// Timestamp for when the API key was created.
        /// </summary>
        public DateTime? Created { get; set; }

        /// <summary>
        /// Timestamp for when the API key was last updated.
        /// </summary>
        public DateTime? LastUpdate { get; set; }

        /// <summary>
        /// Timestamp for when the API key expires.
        /// </summary>
        public DateTime? Expiration { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public ApiKeyPermission()
        {

        }
         
        /// <summary>
        /// Retrieve default permit permission object for the specified user.
        /// </summary>
        /// <param name="curr">UserMaster.</param>
        /// <returns>ApiKeyPermission.</returns>
        public static ApiKeyPermission DefaultPermit(UserMaster curr)
        {
            if (curr == null) throw new ArgumentNullException(nameof(curr));
            ApiKeyPermission ret = new ApiKeyPermission();
            ret.ApiKeyPermissionId = 0;
            ret.ApiKeyId = 0;
            ret.UserMasterId = Convert.ToInt32(curr.UserMasterId);
            ret.ReadObject = true;
            ret.ReadContainer = true;
            ret.WriteObject = true;
            ret.WriteContainer = true;
            ret.DeleteObject = true;
            ret.DeleteContainer = true; 
            return ret;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
