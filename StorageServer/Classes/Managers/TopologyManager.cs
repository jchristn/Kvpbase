using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;
using RestWrapper;

namespace Kvpbase
{
    public class TopologyManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private Events _Logging;
        private Topology _Topology;
        private Node _Node;
        private UserManager _Users;

        private readonly object _Lock;

        #endregion

        #region Constructors-and-Factories

        public TopologyManager(Settings settings, Events logging, Topology topology, Node node, UserManager users)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (topology == null) throw new ArgumentNullException(nameof(topology));
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (users == null) throw new ArgumentNullException(nameof(users));

            _Settings = settings;
            _Logging = logging;
            _Topology = topology;
            _Node = node;
            _Users = users;

            _Lock = new object();

            if (_Topology.Nodes.Count > 0)
            {
                Task.Run(() => Worker());
            }
        }

        #endregion

        #region Public-Methods

        public List<Node> GetNodes()
        {
            lock (_Lock)
            {
                return _Topology.Nodes;
            }
        }

        public List<Node> GetReplicas()
        {
            lock (_Lock)
            {
                return _Topology.Replicas;
            }
        }

        public bool IsNeighbor(int nodeId)
        {
            if (_Node.Neighbors == null) return false;

            lock (_Lock)
            {
                foreach (int currId in _Node.Neighbors)
                {
                    if (currId == nodeId) return true;
                }
            }

            return false;
        }

        public bool IsNeighbor(Node node)
        {
            if (node == null) return false;

            lock (_Lock)
            {
                foreach (int currId in _Node.Neighbors)
                {
                    if (currId == node.NodeId) return true;
                }
            }

            return false;
        }

        public Node DetermineOwner(string userGuid)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(userGuid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DetermineOwner null user GUID supplied");
                return null;
            }

            if ((_Topology == null)
                || (_Topology.Nodes == null)
                || (_Topology.Nodes.Count < 1))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DetermineOwner null topology or no nodes in topology");
                return null;
            }

            #endregion

            #region Find-if-Static-Map

            UserMaster currUser = _Users.GetUserByGuid(userGuid);
            if (currUser != null)
            {
                if (currUser.NodeId > 0)
                {
                    #region Static-Map

                    if (currUser.NodeId == _Node.NodeId)
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "DetermineOwner GUID " + userGuid + " statically mapped to self (NodeId " + _Node.NodeId + ")");
                        return _Node;
                    }

                    lock (_Lock)
                    {
                        foreach (Node curr in _Topology.Nodes)
                        {
                            if (curr.NodeId == currUser.NodeId)
                            {
                                _Logging.Log(LoggingModule.Severity.Debug, "DetermineOwner GUID " + userGuid + " statically mapped to NodeId " + curr.NodeId);
                                return curr;
                            }
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
            List<Node> sortedList = null;
            lock (_Lock)
            {
                 sortedList = _Topology.Nodes.OrderBy(o => o.Name).ToList();
            }

            // determine modulus
            matchPos = Common.GuidToInt(userGuid) % sortedList.Count;

            foreach (Node curr in sortedList)
            {
                if (currPos == matchPos)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "DetermineOwner primary for user GUID " + userGuid + " is " + curr.Name + " (" + curr.DnsHostname + ":" + curr.Port + ":" + curr.Ssl + ")");
                    return curr;
                }

                currPos++;
            }

            _Logging.Log(LoggingModule.Severity.Warn, "DetermineOwner iterated all nodes in sorted list, did not encounter " + matchPos + " entries");
            return null;

            #endregion
        }

        public bool FindObject(Find req)
        {
            #region Set-URL

            string url = "";
            if (Common.IsTrue(_Node.Ssl))
            {
                url = "https://" + _Node.DnsHostname + ":" + _Node.Port + "/admin/find";
            }
            else
            {
                url = "http://" + _Node.DnsHostname + ":" + _Node.Port + "/admin/find";
            }

            #endregion

            #region Headers

            Dictionary<string, string> headers = Common.AddToDictionary(_Settings.Server.HeaderApiKey, _Settings.Server.AdminApiKey, null);

            #endregion

            #region Override-Query-Topology

            req.QueryTopology = false;

            #endregion

            #region Process-Request

            RestWrapper.RestResponse resp = RestRequest.SendRequestSafe(
                url, "application/json", "POST", null, null, false,
                Common.IsTrue(_Settings.Rest.AcceptInvalidCerts), headers,
                Encoding.UTF8.GetBytes(Common.SerializeJson(req)));

            if (resp == null) return false;
            if (resp.StatusCode != 200) return false;

            #endregion

            return true;
        }

        #endregion

        #region Private-Methods

        private void Worker()
        {
            #region Process

            while (true)
            {
                #region Wait

                Task.Delay(_Settings.Topology.RefreshSec * 1000).Wait();

                #endregion

                #region Session-Variables

                List<Node> sourceNodeList = new List<Node>();
                List<Node> updatedNodeList = new List<Node>();
                List<Node> updatedNeighborList = new List<Node>();

                #endregion

                #region Process-the-List
                
                lock (_Lock)
                {
                    sourceNodeList = new List<Node>(GetNodes());
                }

                foreach (Node curr in sourceNodeList)
                {
                    #region Skip-if-Self

                    if (_Node.NodeId == curr.NodeId)
                    {
                        updatedNodeList.Add(curr);
                        continue;
                    }

                    #endregion

                    #region Set-URL

                    string url = "";
                    if (Common.IsTrue(curr.Ssl))
                    {
                        url = "https://" + curr.DnsHostname + ":" + curr.Port + "/admin/heartbeat";
                    }
                    else
                    {
                        url = "http://" + curr.DnsHostname + ":" + curr.Port + "/admin/heartbeat";
                    }

                    #endregion

                    #region Process-REST-Request

                    RestWrapper.RestResponse resp = RestRequest.SendRequestSafe(
                        url, "application/json", "GET", null, null, false,
                        Common.IsTrue(_Settings.Rest.AcceptInvalidCerts),
                        Common.AddToDictionary(_Settings.Server.HeaderApiKey, _Settings.Server.AdminApiKey, null),
                        null);

                    if (resp == null)
                    {
                        #region No-REST-Response

                        curr.NumFailures++;
                        curr.LastAttempt = DateTime.Now;
                        _Logging.Log(LoggingModule.Severity.Warn, "PeerManagerWorker null response connecting to " + url + " (" + curr.NumFailures + " failed attempts)");
                        updatedNodeList.Add(curr);
                        if (IsNeighbor(curr)) updatedNeighborList.Add(curr);
                        continue;

                        #endregion
                    }
                    else
                    {
                        if (resp.StatusCode != 200)
                        {
                            #region Failed-Heartbeat

                            curr.NumFailures++;
                            curr.LastAttempt = DateTime.Now;
                            _Logging.Log(LoggingModule.Severity.Warn, "PeerManagerWorker non-200 (" + resp.StatusCode + ") response connecting to " + url + " (" + curr.NumFailures + " failed attempts)");
                            updatedNodeList.Add(curr);
                            if (IsNeighbor(curr)) updatedNeighborList.Add(curr);
                            continue;

                            #endregion
                        }
                        else
                        {
                            #region Successful-Heartbeat

                            curr.NumFailures = 0;
                            curr.LastAttempt = DateTime.Now;
                            curr.LastSuccess = DateTime.Now;
                            updatedNodeList.Add(curr);
                            if (IsNeighbor(curr)) updatedNeighborList.Add(curr);
                            continue;

                            #endregion
                        }
                    }

                    #endregion
                }

                #endregion

                #region Update-the-Lists

                lock (_Lock)
                {
                    _Topology.Nodes = updatedNodeList;
                    _Topology.Replicas = updatedNeighborList;
                    _Topology.LastProcessed = DateTime.Now;
                }

                #endregion
            }

            #endregion
        }

        #endregion
    }
}
