using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using SyslogLogging;
using RestWrapper;

namespace Kvpbase
{
    public class Node
    {
        #region Public-Members

        public int NodeId { get; set; }
        public string Name { get; set; }
        public string DnsHostname { get; set; }
        public int Port { get; set; }
        public int Ssl { get; set; }
        public List<int> Neighbors { get; set; }
        public DateTime? LastSuccess { get; set; }
        public DateTime? LastAttempt { get; set; }
        public int NumFailures { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public Node()
        {

        }

        #endregion

        #region Public-Methods

        public override string ToString()
        {
            string ret = "";
            ret += "  Node ID " + NodeId + " Name " + Name + " Endpoint " + DnsHostname + ":" + Port + " SSL " + Ssl;
            return ret;
        }

        #endregion

        #region Private-Methods
        
        #endregion
    }
}
