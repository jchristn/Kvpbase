using System;
using WatsonWebserver;

namespace Kvpbase.StorageServer.Classes
{
    /// <summary>
    /// An HTTP connection.
    /// </summary>
    public class Connection
    { 
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
        /// User GUID.
        /// </summary>
        public string UserGUID { get; set; }
         
        /// <summary>
        /// HTTP method
        /// </summary>
        public HttpMethod Method { get; set; }

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
         
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public Connection()
        {

        } 
    }
}
