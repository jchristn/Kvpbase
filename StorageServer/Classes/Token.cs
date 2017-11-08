using System;
using SyslogLogging;

namespace Kvpbase
{
    public class Token
    {
        #region Public-Members

        public int? UserMasterId { get; set; }
        public string Email { get; set; }
        public string Password { get; set; }
        public string Guid { get; set; }
        public string Random { get; set; }
        public DateTime? Expiration { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

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
