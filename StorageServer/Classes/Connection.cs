using System;
using SyslogLogging;

namespace Kvpbase
{
    public class Connection
    {
        #region Public-Members

        public int ThreadId { get; set; }
        public string SourceIp { get; set; }
        public int SourcePort { get; set; }
        public int UserMasterId { get; set; }
        public string Email { get; set; }
        public string Method { get; set; }
        public string RawUrl { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

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
