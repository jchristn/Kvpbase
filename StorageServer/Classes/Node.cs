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

        public bool IsNeighbor(int nodeId)
        {
            if (Neighbors == null) return false;

            foreach (int currId in Neighbors)
            {
                if (currId == nodeId) return true;
            }

            return false;
        }

        public bool IsNeighbor(Node node)
        {
            if (node == null) return false;
            return IsNeighbor(node.NodeId);
        }

        #endregion

        #region Public-Static-Methods

        public static Node DetermineOwner(string userGuid, UserManager users, Topology topology, Node currentNode, Events logging)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(userGuid))
            {
                logging.Log(LoggingModule.Severity.Warn, "DetermineOwner null user GUID supplied");
                return null;
            }

            if ((topology == null)
                || (topology.Nodes == null)
                || (topology.Nodes.Count < 1))
            {
                logging.Log(LoggingModule.Severity.Warn, "DetermineOwner null topology or no nodes in topology");
                return null;
            }

            #endregion

            #region Find-if-Static-Map

            UserMaster currUser = users.GetUserByGuid(userGuid);
            if (currUser != null)
            {
                if (currUser.NodeId > 0)
                {
                    #region Static-Map

                    if (currUser.NodeId == currentNode.NodeId)
                    {
                        logging.Log(LoggingModule.Severity.Debug, "DetermineOwner GUID " + userGuid + " statically mapped to self (NodeId " + currentNode.NodeId + ")");
                        return currentNode;
                    }

                    foreach (Node curr in topology.Nodes)
                    {
                        if (curr.NodeId == currUser.NodeId)
                        {
                            logging.Log(LoggingModule.Severity.Debug, "DetermineOwner GUID " + userGuid + " statically mapped to NodeId " + curr.NodeId);
                            return curr;
                        }
                    }

                    #endregion
                }
            }

            #endregion

            #region No-Static-Map-Exists

            int currPos = 0;
            int matchPos = 0;

            // sort the list by name
            List<Node> sorted_list = topology.Nodes.OrderBy(o => o.Name).ToList();

            // determine modulus
            matchPos = Common.GuidToInt(userGuid) % sorted_list.Count;

            foreach (Node curr in sorted_list)
            {
                if (currPos == matchPos)
                {
                    logging.Log(LoggingModule.Severity.Debug, "DetermineOwner primary for user GUID " + userGuid + " is " + curr.Name + " (" + curr.DnsHostname + ":" + curr.Port + ":" + curr.Ssl + ")");
                    return curr;
                }

                currPos++;
            }

            logging.Log(LoggingModule.Severity.Warn, "DetermineOwner iterated all nodes in sorted list, did not encounter " + matchPos + " entries");
            return null;

            #endregion
        }

        public static bool FindObject(Settings settings, Node curr, Find req)
        {
            #region Set-URL

            string url = "";
            if (Common.IsTrue(curr.Ssl))
            {
                url = "https://" + curr.DnsHostname + ":" + curr.Port + "/admin/find";
            }
            else
            {
                url = "http://" + curr.DnsHostname + ":" + curr.Port + "/admin/find";
            }

            #endregion

            #region Headers

            Dictionary<string, string> headers = Common.AddToDictionary(settings.Server.HeaderApiKey, settings.Server.AdminApiKey, null);

            #endregion

            #region Override-Query-Topology

            req.QueryTopology = false;

            #endregion

            #region Process-Request

            RestWrapper.RestResponse resp = RestRequest.SendRequestSafe(
                url, "application/json", "POST", null, null, false,
                Common.IsTrue(settings.Rest.AcceptInvalidCerts), headers,
                Encoding.UTF8.GetBytes(Common.SerializeJson(req)));

            if (resp == null) return false;
            if (resp.StatusCode != 200) return false;

            #endregion

            return true;
        }

        #endregion
    }
}
