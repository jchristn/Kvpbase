using System; 

namespace Kvpbase
{
    /// <summary>
    /// Metadata object for encrypted messages and data.
    /// </summary>
    public class EncryptedMessage
    {
        #region Public-Members

        /// <summary>
        /// Cleartext data.
        /// </summary>
        public byte[] Clear { get; set; }

        /// <summary>
        /// Enciphered data.
        /// </summary>
        public byte[] Cipher { get; set; }

        /// <summary>
        /// Key sequence number used during encryption.
        /// </summary>
        public string Ksn { get; set; }

        /// <summary>
        /// Start time of the encryption or decryption process.
        /// </summary>
        public DateTime? StartTime { get; set; }

        /// <summary>
        /// End time of the encryption or decryption process.
        /// </summary>
        public DateTime? EndTime { get; set; }

        /// <summary>
        /// Number of elapsed milliseconds during the encryption or decryption process.
        /// </summary>
        public decimal TotalMilliseconds { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public EncryptedMessage()
        {

        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
