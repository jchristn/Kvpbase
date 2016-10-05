using SyslogLogging;

namespace Kvpbase
{
    public class Heartbeat
    {
        #region Public-Members

        public int NodeId { get; set; }
        public string Name { get; set; }
        public string DnsHostname { get; set; }
        public int Port { get; set; }
        public int Ssl { get; set; }

        #endregion

        #region Constructors-and-Factories

        public Heartbeat()
        {

        }

        #endregion
    }
}
