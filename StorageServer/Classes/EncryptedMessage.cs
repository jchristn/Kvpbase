using System;
using SyslogLogging;

namespace Kvpbase
{
    public class EncryptedMessage
    {
        #region Public-Members

        public byte[] Clear { get; set; }
        public byte[] Cipher { get; set; }
        public string Ksn { get; set; }
        public DateTime? StartTime { get; set; }
        public DateTime? EndTime { get; set; }
        public decimal TotalMilliseconds { get; set; }

        #endregion

        #region Constructors-and-Factories

        public EncryptedMessage()
        {

        }

        #endregion
    }
}
