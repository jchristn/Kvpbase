using System; 

namespace Kvpbase
{
    /// <summary>
    /// An HTTP connection.
    /// </summary>
    public class Connection
    {
        #region Public-Members

        /// <summary>
        /// Thread ID of the connection.
        /// </summary>
        public int ThreadId { get; set; }

        /// <summary>
        /// Source IP address.
        /// </summary>
        public string SourceIp { get; set; }

        /// <summary>
        /// Source TCP port.
        /// </summary>
        public int SourcePort { get; set; }

        /// <summary>
        /// User ID of the requestor.
        /// </summary>
        public int? UserMasterId { get; set; }

        /// <summary>
        /// Email address of the requestor.
        /// </summary>
        public string Email { get; set; }

        /// <summary>
        /// HTTP method
        /// </summary>
        public string Method { get; set; }

        /// <summary>
        /// URL being accessed.
        /// </summary>
        public string RawUrl { get; set; }

        /// <summary>
        /// When the connection was received.
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// When the connection was terminated.
        /// </summary>
        public DateTime EndTime { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public Connection()
        {

        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
