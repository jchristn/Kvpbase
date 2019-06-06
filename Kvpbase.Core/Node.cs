using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;  

namespace Kvpbase.Core
{
    /// <summary>
    /// A node on the Kvpbase peer-to-peer mesh network.
    /// </summary>
    public class Node
    {
        #region Public-Members

        /// <summary>
        /// ID of the node.
        /// </summary>
        public int NodeId { get; set; }

        /// <summary>
        /// Name of the node.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// HTTP server settings.
        /// </summary>
        public HttpSettings Http { get; set; }

        /// <summary>
        /// TCP server settings.
        /// </summary>
        public TcpSettings Tcp { get; set; } 

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public Node()
        {

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
            ret += "ID " + NodeId + " Name " + Name;

            if (Http != null)
            {
                if (!Http.Ssl) ret += " http://" + Http.DnsHostname + ":" + Http.Port;
                else ret += " https://" + Http.DnsHostname + ":" + Http.Port;
            }

            if (Tcp != null)
            {
                if (!Tcp.Ssl) ret += " tcp://" + Tcp.IpAddress + ":" + Tcp.Port;
                else ret += " tcps://" + Tcp.IpAddress + ":" + Tcp.Port;
            }

            return ret;
        }

        /// <summary>
        /// Return a sanitized Node object, which can then be serialized.
        /// Certain properties within HttpRequest cannot be serialized, so objects must be sanitized first.
        /// </summary>
        /// <returns>Serializable version of the object.</returns>
        public Node Sanitized()
        {
            Node ret = new Node();
            ret.NodeId = NodeId;
            ret.Name = Name;
            ret.Http = Http;
            ret.Tcp = Tcp;
            if (ret.Tcp != null)
            {
                ret.Tcp.PfxCertificateFile = null;
                ret.Tcp.PfxCertificatePass = null;
            }
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion

        #region Public-Embedded-Classes

        /// <summary>
        /// HTTP API server settings.
        /// </summary>
        public class HttpSettings
        {
            /// <summary>
            /// The port number on which the server will listen.
            /// </summary>
            public int Port { get; set; }

            /// <summary>
            /// The DNS hostname that must be found on incoming HTTP requests in the 'Host' header.
            /// </summary>
            public string DnsHostname { get; set; } 

            /// <summary>
            /// Enable or disable SSL.
            /// </summary>
            public bool Ssl { get; set; }
        }

        /// <summary>
        /// TCP server settings.
        /// </summary>
        public class TcpSettings
        {
            /// <summary>
            /// The IP address on which the server will listen.
            /// </summary>
            public string IpAddress { get; set; }

            /// <summary>
            /// The port number on which the server will listen.
            /// </summary>
            public int Port { get; set; }

            /// <summary>
            /// Enable or disable SSL.
            /// </summary>
            public bool Ssl { get; set; }

            /// <summary>
            /// PFX certificate path and filename.
            /// </summary>
            public string PfxCertificateFile { get; set; }

            /// <summary>
            /// Password for the PFX certificate file.
            /// </summary>
            public string PfxCertificatePass { get; set; }

            /// <summary>
            /// Enable to accept invalid or unverifiable SSL certificates.
            /// </summary>
            public bool AcceptInvalidCertificates { get; set; }

            /// <summary>
            /// Require mutual SSL authentication.
            /// </summary>
            public bool MutuallyAuthenticate { get; set; }
            
            /// <summary>
            /// Enable to read the full data stream from incoming messages (not recommended).
            /// </summary>
            public bool ReadDataStream { get; set; }

            /// <summary>
            /// Buffer size to use while reading the stream.
            /// </summary>
            public int StreamReadBufferSize { get; set; }

            /// <summary>
            /// Timeout settings for mesh networking exchanges.
            /// </summary>
            public TimeoutSettings Timeout { get; set; }

            /// <summary>
            /// Timeout settings for mesh networking exchanges.
            /// </summary>
            public class TimeoutSettings
            {
                /// <summary>
                /// Minimum timeout for synchronous messages.  Also used for simple messages with no data payload.
                /// </summary>
                public int MinTimeoutSec { get; set; }

                /// <summary>
                /// Maximum timeout for synchronous messages.  Messages with data payload will have a timeout set no higher than this value.
                /// </summary>
                public int MaxTimeoutSec { get; set; }

                /// <summary>
                /// Expected data transfer rate in bytes per second.  Messages with data payload will calculate a timeout based on this value, and be bounded by MinTimeoutSec and MaxTimeoutSec.
                /// </summary>
                public int ExpectedXferRateBytesPerSec { get; set; }
            }
        }

        #endregion
    }
}
