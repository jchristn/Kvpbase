using System;
using System.Text;
using SyslogLogging;
using RestWrapper;

namespace Kvpbase
{
    public class Message
    {
        #region Public-Members

        public Node From { get; set; }
        public Node To { get; set; }
        public string Subject { get; set; }
        public string Data { get; set; }
        public DateTime? Created { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public Message()
        {

        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
