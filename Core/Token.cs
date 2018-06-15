using System; 

namespace Kvpbase
{
    /// <summary>
    /// A secure authentication token.
    /// </summary>
    public class Token
    {
        #region Public-Members

        /// <summary>
        /// The ID of the user.
        /// </summary>
        public int? UserMasterId { get; set; }

        /// <summary>
        /// The user's email address.
        /// </summary>
        public string Email { get; set; }

        /// <summary>
        /// The user's password.
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// The user's GUID.
        /// </summary>
        public string Guid { get; set; }

        /// <summary>
        /// Random string data, useful for protecting secrecy of the encryption material.
        /// </summary>
        public string Random { get; set; }

        /// <summary>
        /// The time at which the token will expire.
        /// </summary>
        public DateTime? Expiration { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public Token()
        {

        }

        #endregion

        #region Public-Methods
         
        #endregion

        #region Private-Methods

        #endregion
    }
}
