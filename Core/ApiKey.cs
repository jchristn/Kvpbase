using System;
using System.Collections.Generic;
using System.IO; 

namespace Kvpbase
{
    /// <summary>
    /// API key for use when accessing the RESTful HTTP API.
    /// </summary>
    public class ApiKey
    {
        #region Public-Members

        /// <summary>
        /// ID of the API key.
        /// </summary>
        public int? ApiKeyId { get; set; }

        /// <summary>
        /// ID of the user to which the API key is mapped.
        /// </summary>
        public int? UserMasterId { get; set; }
         
        /// <summary>
        /// The API key itself (in GUID format).
        /// </summary>
        public string Guid { get; set; }

        /// <summary>
        /// Administrator notes for the API key.
        /// </summary>
        public string Notes { get; set; }

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
        public ApiKey()
        {

        }
         
        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
