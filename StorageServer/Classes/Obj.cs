using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public class Obj
    {
        #region Public-Members

        public string UserGuid { get; set; }
        public string Key { get; set; }
        public Node PrimaryNode { get; set; }
        public string PrimaryUrlWithQs { get; set; }
        public string PrimaryUrlWithoutQs { get; set; }
        public string ReplicationMode { get; set; }
        public List<Node> Replicas { get; set; }
        public string ContentType { get; set; }
        public DateTime? Created { get; set; }
        public DateTime? LastUpdate { get; set; }
        public DateTime? LastAccess { get; set; }
        public DateTime? Expiration { get; set; }
        public int? IsCompressed { get; set; }
        public int? IsEncrypted { get; set; }
        public int? IsEncoded { get; set; }
        public string EncryptionKsn { get; set; }
        public int? IsContainer { get; set; }
        public int? IsObject { get; set; }
        public int? GatewayMode { get; set; }
        public List<string> ContainerPath { get; set; }
        public List<string> Tags { get; set; }
        public string DiskPath { get; set; }
        public byte[] Value { get; set; }
        public string Md5Hash { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public Obj()
        {

        }

        #endregion

        #region Public-Methods

        public override string ToString()
        {
            string ret = "";

            ret += "Key " + Key + " User GUID " + UserGuid + Environment.NewLine;
            ret += "  Primary Node: " + PrimaryNode.ToString();
            ret += "  Primary URL with Query    : " + PrimaryUrlWithQs + Environment.NewLine;
            ret += "  Primary URL without Query : " + PrimaryUrlWithoutQs + Environment.NewLine;

            if (Replicas != null)
            {
                ret += "  Replicas   : " + Environment.NewLine;
                foreach (Node curr in Replicas) ret += "  " + curr.ToString();
            }
            else
            {
                ret += "  Replicas: none" + Environment.NewLine;
            }

            if (IsCompressed != null) ret += "  Compressed : " + IsCompressed + Environment.NewLine;
            if (IsEncrypted != null) ret += "  Encrypted  : " + IsEncrypted + Environment.NewLine;
            if (IsEncoded != null) ret += "  Encoded    : " + IsEncoded + Environment.NewLine;
            ret += "  Disk Path  : " + DiskPath + Environment.NewLine;
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion 
    }
}
