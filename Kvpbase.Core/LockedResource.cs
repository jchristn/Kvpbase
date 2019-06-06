using System;
using System.Collections.Generic;
using System.IO;
using WatsonWebserver;

namespace Kvpbase.Core
{
    /// <summary>
    /// Metadata about resources (objects) in use by users.
    /// </summary>
    public class LockedResource
    {
        #region Public-Members

        /// <summary>
        /// The ID of the user.
        /// </summary>
        public int? UserMasterId { get; set; }

        /// <summary>
        /// The HTTP method in use.
        /// </summary>
        public HttpMethod Method { get; set; }

        /// <summary>
        /// The URL of the resource in use.
        /// </summary>
        public string Url { get; set; }

        /// <summary>
        /// The time at which the request was received.
        /// </summary>
        public DateTime Created { get; set; }

        #endregion

        #region Private-Members

        private static string _TimestampFormat = "yyyy-MM-ddTHH:mm:ss.ffffffZ";

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        /// <param name="user">UserMaster.</param>
        /// <param name="method">HTTP method.</param>
        /// <param name="url">Object URL.</param>
        public LockedResource(UserMaster user, HttpMethod method, string url)
        {
            if (user != null) UserMasterId = user.UserMasterId;
            else UserMasterId = null;

            Method = method;
            Url = url;
            Created = DateTime.Now.ToUniversalTime();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a human-readable string of the object.
        /// </summary>
        /// <returns>String.</returns>
        public override string ToString()
        {
            string ret = "";

            ret += "User ";
            if (UserMasterId != null) ret += UserMasterId + " ";
            else ret += "[null] ";

            ret += Method + " " + Url + " ";
            ret += "[" + Created.ToString(_TimestampFormat) + "]";

            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
