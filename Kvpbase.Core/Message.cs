using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks; 

namespace Kvpbase.Core
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
        /// Length of the data or data stream.
        /// </summary>
        public long ContentLength { get; set; }
         
        /// <summary>
        /// Stream containing the data associated with the request.
        /// </summary>
        public Stream DataStream { get; set; }

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
        /// <param name="md">Request metadata associated with the message.</param>
        /// <param name="msgType">The type of message.</param>
        /// <param name="success">Indicates whether or not the operation succeeded (set only in responses).</param>
        /// <param name="contentLength">Content length.</param>
        /// <param name="stream">Stream containing the data associated with the request.</param>
        public Message(Node from, Node to, RequestMetadata md, MessageType msgType, bool? success, long contentLength, Stream stream)
        { 
            if (from == null) throw new ArgumentNullException(nameof(from)); 
            if (to == null) throw new ArgumentNullException(nameof(to)); 
            if (stream != null && stream.CanSeek) stream.Seek(0, SeekOrigin.Begin);

            From = from; 
            To = to; 
            Metadata = md; 
            Type = msgType; 
            Success = success; 
            ContentLength = contentLength;
            DataStream = stream;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a human-readable string of the object.
        /// </summary>
        /// <returns>String.</returns>
        public override string ToString()
        {
            string ret =
                "---" + Environment.NewLine;

            if (From != null)
                ret += "  From          : " + From.ToString() + Environment.NewLine;

            if (To != null)
                ret += "  To            : " + To.ToString() + Environment.NewLine;

            ret += "  MessageType   : " + Type.ToString() + Environment.NewLine;

            if (Success != null)
                ret += "  Success       : " + Success + Environment.NewLine;
             
            ret += "  ContentLength : " + ContentLength + Environment.NewLine;

            if (Metadata != null)
                ret += "  Metadata      : " + Common.SerializeJson(Metadata, true) + Environment.NewLine;
            else
                ret += "  Metadata      : null";

            if (DataStream != null)
                ret += "  DataStream    : [present]" + Environment.NewLine;
             
            return ret; 
        }

        /// <summary>
        /// Return a byte array containing the headers of the message without Data.
        /// </summary>
        /// <returns>Byte array.</returns>
        public byte[] ToHeaderBytes()
        {
            string str = "";
            if (From != null)
            {
                str += "From: " + Common.SerializeJson(From, false) + "\r\n";
            }

            if (To != null)
            {
                str += "To: " + Common.SerializeJson(To, false) + "\r\n";
            }

            if (Metadata != null)
            {
                str += "Metadata: " + Common.SerializeJson(Metadata, false) + "\r\n";
            }

            str += "Type: " + Type.ToString() + "\r\n";

            if (Success != null)
            {
                str += "Success: " + Success.ToString() + "\r\n";
            }

            str += "ContentLength: " + ContentLength.ToString() + "\r\n";
            str += "\r\n";
             
            return Encoding.UTF8.GetBytes(str); 
        }
         
        /// <summary>
        /// Build a Message from a supplied stream.
        /// </summary>
        /// <param name="stream">Stream containing the data.</param>
        /// <param name="readData">Indicate whether or not content should be read and placed in Data.</param>
        /// <returns>Message.</returns>
        public static Message FromStream(Stream stream)
        { 
            if (stream == null) throw new ArgumentNullException(nameof(stream)); 
            if (!stream.CanRead) throw new ArgumentException("Cannot read from supplied stream.");

            try
            {
                stream.Seek(0, SeekOrigin.Begin);

                Message ret = new Message();

                #region Headers

                int headerBytesRead = 0;
                byte[] headerBuffer = new byte[1];
                string headers = "";

                while (!headers.EndsWith("\r\n\r\n"))
                {
                    headerBytesRead = stream.Read(headerBuffer, 0, headerBuffer.Length);
                    if (headerBytesRead > 0)
                    {
                        headers += Encoding.UTF8.GetString(headerBuffer);
                    }
                }

                #endregion

                #region Parse-Headers

                string[] headerLines = headers.Split(new[] { Environment.NewLine }, StringSplitOptions.None);
                if (headerLines.Length > 0)
                {
                    foreach (string currHeaderLine in headerLines)
                    {
                        string[] headerParts = currHeaderLine.Split(new[] { ':' }, 2);
                        if (headerParts.Length != 2) continue;

                        string key = headerParts[0];
                        string val = headerParts[1];

                        if (!String.IsNullOrEmpty(key)) key.Trim();

                        if (!String.IsNullOrEmpty(val))
                        {
                            val.Trim();

                            switch (key)
                            {
                                case "From":
                                    ret.From = Common.DeserializeJson<Node>(val);
                                    break;
                                case "To":
                                    ret.To = Common.DeserializeJson<Node>(val);
                                    break;
                                case "Metadata":
                                    ret.Metadata = Common.DeserializeJson<RequestMetadata>(val);
                                    break;
                                case "Type":
                                    ret.Type = (MessageType)(Enum.Parse(typeof(MessageType), val));
                                    break;
                                case "Success":
                                    ret.Success = Convert.ToBoolean(val);
                                    break;
                                case "ContentLength":
                                    ret.ContentLength = Convert.ToInt64(val);
                                    break;

                            }
                        }
                    }
                }

                #endregion

                #region Data

                if (ret.ContentLength > 0)
                {
                    int bytesRead = 0;
                    long bytesRemaining = ret.ContentLength;
                    byte[] buffer = new byte[65536];
                    ret.DataStream = new MemoryStream();

                    while (bytesRemaining > 0)
                    {
                        bytesRead = stream.Read(buffer, 0, buffer.Length);
                        if (bytesRead > 0)
                        {
                            ret.DataStream.Write(buffer, 0, bytesRead);
                            bytesRemaining -= bytesRead;
                        }
                    }

                    if (ret.DataStream.CanSeek) ret.DataStream.Seek(0, SeekOrigin.Begin);
                }

                #endregion

                return ret;
            }
            catch (Exception)
            { 
                return null;
            }
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
