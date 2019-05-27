using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks; 

namespace Kvpbase
{
    /// <summary>
    /// Message exchanged between Kvpbase nodes over the peer-to-peer mesh.
    /// </summary>
    public class Message
    {
        #region Public-Members

        /// <summary>
        /// Sending node.
        /// </summary>
        public Node From { get; set; }

        /// <summary>
        /// Recipient node.
        /// </summary>
        public Node To { get; set; }

        /// <summary>
        /// Request metadata associated with the message.
        /// </summary>
        public RequestMetadata Metadata { get; set; }

        /// <summary>
        /// The type of message.
        /// </summary>
        public MessageType Type { get; set; } 

        /// <summary>
        /// Indicates whether or not the operation succeeded (set only in responses).
        /// </summary>
        public bool? Success { get; set; }

        /// <summary>
        /// Data associated with the request.
        /// </summary>
        public byte[] Data { get; set; }
         
        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public Message()
        {

        }

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        /// <param name="from">Sending node.</param>
        /// <param name="to">Recipient node.</param>
        /// <param name="msgType">The type of message.</param>
        /// <param name="success">Indicates whether or not the operation succeeded (set only in responses).</param>
        /// <param name="data">Data associated with the request.</param>
        public Message(Node from, Node to, MessageType msgType, bool? success, byte[] data)
        {
            if (from == null) throw new ArgumentNullException(nameof(from));
            if (to == null) throw new ArgumentNullException(nameof(to));

            From = from;
            To = to;
            Metadata = null;
            Type = msgType;
            Success = success;
            Data = data; 
        }

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        /// <param name="from">Sending node.</param>
        /// <param name="to">Recipient node.</param>
        /// <param name="md">Request metadata associated with the message.</param>
        /// <param name="msgType">The type of message.</param>
        /// <param name="success">Indicates whether or not the operation succeeded (set only in responses).</param>
        /// <param name="data">Data associated with the request.</param>
        public Message(Node from, Node to, RequestMetadata md, MessageType msgType, bool? success, byte[] data)
        {
            if (from == null) throw new ArgumentNullException(nameof(from));
            if (to == null) throw new ArgumentNullException(nameof(to));
            if (md == null) throw new ArgumentNullException(nameof(md));

            From = from;
            To = to;
            Metadata = md;
            Type = msgType;
            Success = success;
            Data = data; 
        }
         
        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a human-readable string of the object.
        /// </summary>
        /// <returns>String.</returns>
        public override string ToString()
        {
            return Common.SerializeJson(this, true);
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
