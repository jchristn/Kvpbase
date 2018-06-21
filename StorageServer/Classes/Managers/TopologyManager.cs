using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;
using RestWrapper;
using Watson;

namespace Kvpbase
{
    /// <summary>
    /// Maintains and manages connectivity amongst nodes.  Relies on MessageManager to handle incoming messages.
    /// </summary>
    public class TopologyManager
    {
        #region Public-Members

        public Node LocalNode { get; private set; }

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private Topology _Topology; 
        private UserManager _UserMgr; 
        private MessageManager _MessageMgr;

        private readonly object _TopologyLock;

        private MeshSettings _MeshSettings;
        private Peer _Self;
        private WatsonMesh _Mesh;

        #endregion

        #region Constructors-and-Factories

        public TopologyManager(Settings settings, LoggingModule logging, UserManager users, MessageManager messages)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging)); 
            if (users == null) throw new ArgumentNullException(nameof(users)); 
            if (messages == null) throw new ArgumentNullException(nameof(messages));

            _Settings = settings;
            _Logging = logging; 
            _UserMgr = users; 
            _MessageMgr = messages;

            _TopologyLock = new object();

            LoadTopologyFile();
            SetLocalNode();

            string error;
            if (!ValidateTopology(out error)) throw new Exception("Unable to validate topology: " + error);
             
            InitializeMeshNetwork(); 

            if (_Settings.Topology.DebugMeshNetworking) _Logging.Log(LoggingModule.Severity.Info, "TopologyManager debugging enabled, disable to reduce log verbocity");
            SayHello();
        }

        #endregion

        #region Public-Methods

        public bool IsEmpty()
        {
            if (_Topology.Nodes == null || _Topology.Nodes.Count < 2)
            {
                return true;
            }
            return false;
        }

        public List<Node> GetNodes()
        {
            lock (_TopologyLock)
            {
                return _Topology.Nodes;
            }
        }

        public List<Node> GetReplicas()
        {
            List<Node> ret = new List<Node>();

            lock (_TopologyLock)
            {
                if (_Topology.Nodes == null || _Topology.Nodes.Count < 1) return null;

                foreach (Node curr in _Topology.Nodes)
                {
                    if (_Topology.Replicas.Contains(curr.NodeId))
                    {
                        ret.Add(curr);
                    }
                }
            }

            return ret;
        }
         
        public Node DetermineOwner(string userGuid)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));

            if ((_Topology == null)
                || (_Topology.Nodes == null)
                || (_Topology.Nodes.Count < 1))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DetermineOwner null topology or no nodes in topology");
                return LocalNode;
            }

            #region Check-Static-Map

            UserMaster currUser = _UserMgr.GetUserByGuid(userGuid);
            if (currUser != null)
            {
                if (currUser.NodeId > 0)
                { 
                    if (currUser.NodeId == LocalNode.NodeId)
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "DetermineOwner GUID " + userGuid + " statically mapped to self (NodeId " + LocalNode.NodeId + ")");
                        return LocalNode;
                    }

                    Node currNode = null;

                    lock (_TopologyLock)
                    {
                        currNode = _Topology.Nodes.Where(n => n.NodeId == currUser.NodeId).FirstOrDefault();
                    }

                    if (currNode == default(Node))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "DetermineOwner unable to find node ID " + currUser.NodeId + " for user GUID " + userGuid);
                        return null;
                    }

                    return currNode; 
                }
            }

            #endregion

            #region No-Static-Map-Exists

            int currPos = 0;
            int matchPos = 0;

            // sort the list by name
            List<Node> sortedList = null;
            lock (_TopologyLock)
            {
                 sortedList = _Topology.Nodes.OrderBy(o => o.Name).ToList();
            }

            // determine modulus
            matchPos = Common.GuidToInt(userGuid) % sortedList.Count;

            foreach (Node curr in sortedList)
            {
                if (currPos == matchPos)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "DetermineOwner primary for user GUID " + userGuid + " is " + curr.Name + " (" + curr.Http.DnsHostname + ":" + curr.Http.Port + ")");
                    return curr;
                }

                currPos++;
            }

            _Logging.Log(LoggingModule.Severity.Warn, "DetermineOwner iterated all nodes in sorted list, did not encounter " + matchPos + " entries");
            return null;

            #endregion
        }

        public Node GetNodeById(int nodeId)
        {
            Node ret = null;

            lock (_TopologyLock)
            {
                ret = _Topology.Nodes.Where(n => n.NodeId.Equals(nodeId)).FirstOrDefault();
            }

            if (ret == default(Node)) return null;
            return ret;
        }

        public void AddNode(Node node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            if (NodeExists(node)) return;

            lock (_TopologyLock)
            {
                _Topology.Nodes.Add(node);
            }

            Peer peer = BuildPeerFromNode(node);
            if (!_Mesh.Exists(peer)) _Mesh.Add(peer);
        }

        public void RemoveNode(Node node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            if (!NodeExists(node)) return;

            lock (_TopologyLock)
            {
                _Topology.Nodes = _Topology.Nodes.Where(n => !n.Equals(node)).ToList();
                _Topology.Replicas = _Topology.Replicas.Where(n => !n.Equals(node)).ToList();
            }

            Peer peer = BuildPeerFromNode(node);
            _Mesh.Remove(peer);
        }

        public bool NodeExists(Node node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            lock (_TopologyLock)
            {
                return _Topology.Nodes.Any(n => n.Equals(node));
            }
        }
         
        public bool AddReplica(int nodeId)
        {
            Node node = GetNodeById(nodeId);
            if (node == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddReplica unable to find node ID " + nodeId);
                return false;
            }
             
            lock (_TopologyLock)
            {
                if (!_Topology.Replicas.Contains(nodeId))
                {
                    _Topology.Replicas.Add(nodeId);
                }
            }

            Peer peer = BuildPeerFromNode(node);
            if (!_Mesh.Exists(peer)) _Mesh.Add(peer);

            return true;
        }

        public void RemoveReplica(int nodeId)
        {
            Node node = GetNodeById(nodeId);
            if (node == null) return;

            lock (_TopologyLock)
            {
                if (_Topology.Replicas.Contains(nodeId))
                {
                    _Topology.Replicas.Remove(nodeId);
                }
            } 
        }

        public bool ReplicaExists(int nodeId)
        {
            lock (_TopologyLock)
            {
                return _Topology.Replicas.Contains(nodeId);
            }
        }
         
        public bool SendAsyncMessage(MessageType msgType, int nodeId, byte[] data)
        {
            Node rcpt = GetNodeById(nodeId);
            if (rcpt == null || rcpt == default(Node)) return false;

            Message msg = new Message(LocalNode, rcpt, msgType, null, data);
            return SendAsyncMessage(msg);
        }

        public bool SendAsyncMessage(Message msg)
        { 
            if (msg == null) throw new ArgumentNullException(nameof(msg));
            if (msg.To == null) throw new ArgumentException("Message does not contain 'To' node.");

            msg.From = LocalNode;

            if (_Settings.Topology.DebugMessages)
            {
                _Logging.Log(LoggingModule.Severity.Info, 
                    "SendAsyncMessage sending: " + 
                    Environment.NewLine + 
                    Common.SerializeJson(msg, true));
            }

            return _Mesh.SendAsync(
                msg.To.Tcp.IpAddress,
                msg.To.Tcp.Port,
                Encoding.UTF8.GetBytes(Common.SerializeJson(msg, false)));
        }

        public Message SendSyncMessage(MessageType msgType, int nodeId, byte[] data, int timeoutMs)
        {
            Node rcpt = GetNodeById(nodeId);
            if (rcpt == null || rcpt == default(Node)) return null;

            Message msg = new Message(LocalNode, rcpt, msgType, null, data);
            return SendSyncMessage(msg, timeoutMs);
        }

        public Message SendSyncMessage(Message msg, int timeoutMs)
        {
            if (msg == null) throw new ArgumentNullException(nameof(msg));
            if (msg.To == null) throw new ArgumentException("Message does not contain 'To' node.");

            msg.From = LocalNode;

            if (_Settings.Topology.DebugMessages)
            {
                _Logging.Log(LoggingModule.Severity.Info,
                    "SendSyncMessage sending: " +
                    Environment.NewLine +
                    Common.SerializeJson(msg, true));
            }

            byte[] response;
            if (!_Mesh.SendSync(
                msg.To.Tcp.IpAddress, 
                msg.To.Tcp.Port, 
                timeoutMs, 
                Encoding.UTF8.GetBytes(Common.SerializeJson(msg, false)),
                out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSyncMessage unable to send message to node ID " + msg.To.NodeId);
                return null;
            }
             
            if (response == null || response.Length < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSyncMessage no data returned from node ID " + msg.To.NodeId);
                return null;
            }

            try
            {
                Message resp = Common.DeserializeJson<Message>(response);

                if (_Settings.Topology.DebugMessages)
                {
                    _Logging.Log(LoggingModule.Severity.Info,
                        "SendSyncMessage received: " +
                        Environment.NewLine +
                        Common.SerializeJson(resp, true));
                }

                return resp;
            }
            catch (Exception e)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSyncMessage exception while deserializing response: " + e.Message);
                _Logging.Log(LoggingModule.Severity.Warn, Encoding.UTF8.GetString(response));
                return null;
            }
        }

        public void SayHello()
        {
            List<Node> nodes = null;
            lock (_TopologyLock)
            {
                nodes = new List<Node>(_Topology.Nodes);
            }

            if (nodes != null && nodes.Count > 0)
            {
                foreach (Node node in nodes)
                {
                    Message msg = new Message(LocalNode, node, MessageType.Hello, null, Encoding.UTF8.GetBytes("Hello")); 
                    SendAsyncMessage(msg);
                }
            }
        }

        public bool IsNetworkHealthy()
        {
            return _Mesh.IsHealthy();
        }

        public bool IsNodeHealthy(int nodeId)
        {
            Node currNode = GetNodeById(nodeId);
            return IsNodeHealthy(currNode);
        }

        public bool IsNodeHealthy(Node node)
        {
            if (node == null) return false;
            if (node.Tcp.IpAddress.Equals(LocalNode.Tcp.IpAddress) && node.Tcp.Port.Equals(LocalNode.Tcp.Port)) return true;
            return _Mesh.IsHealthy(node.Tcp.IpAddress, node.Tcp.Port);
        }

        public bool AreReplicasHealthy()
        {
            if (_Topology.Replicas == null || _Topology.Replicas.Count < 1) return true;
            
            foreach (int curr in _Topology.Replicas)
            {
                if (!IsNodeHealthy(curr)) return false;
            }

            return true;
        }

        #endregion

        #region Private-Methods

        private void LoadTopologyFile()
        {
            lock (_TopologyLock)
            {
                _Topology = Common.DeserializeJson<Topology>(Common.ReadBinaryFile(_Settings.Files.Topology));
            } 
        }

        private void SetLocalNode()
        {
            LocalNode = null;

            lock (_TopologyLock)
            {
                foreach (Node curr in _Topology.Nodes)
                {
                    if (_Topology.NodeId == curr.NodeId)
                    {
                        LocalNode = curr;
                        break;
                    }
                }
            }

            if (LocalNode == null) throw new Exception("Unable to find local node in topology.");
        }

        private void SaveTopologyFile()
        {
            lock (_TopologyLock)
            {
                Common.WriteFile(
                    _Settings.Files.Topology,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(_Topology, true)));
            }
        }

        private bool ValidateTopology(out string error)
        { 
            error = null;
            LocalNode = null;
            List<int> allNodeIds = new List<int>();

            #region Build-All-Node-ID-List

            if (_Topology.Nodes == null || _Topology.Nodes.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ValidateTopology no nodes in topology");
                error = "No nodes in topology";
                return false;
            }

            if (_Topology.Replicas == null || _Topology.Replicas.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ValidateTopology no node IDs in replica list");
                error = "No replica node IDs specified";
                return false;
            }

            foreach (Node curr in _Topology.Nodes)
            {
                allNodeIds.Add(curr.NodeId);
            }

            #endregion

            #region Find-Current-Node

            bool currentNodeFound = false;

            foreach (Node curr in _Topology.Nodes)
            {
                if (_Topology.NodeId == curr.NodeId)
                {
                    LocalNode = curr;
                    currentNodeFound = true;
                    break;
                }
            }

            if (!currentNodeFound)
            {
                error = "Unable to find local node ID in topology.";
                return false;
            }

            #endregion

            #region Verify-Replicas-Exit

            foreach (int currNodeId in _Topology.Replicas)
            { 
                if (!allNodeIds.Contains(currNodeId))
                { 
                    error = "Replica node ID " + currNodeId + " not found in node list.";
                    return false;
                }
            }

            #endregion

            return true;
        }
        
        private void InitializeMeshNetwork()
        { 
            _MeshSettings = new MeshSettings();
            _MeshSettings.DebugNetworking = _Settings.Topology.DebugMeshNetworking;

            _Self = BuildPeerFromNode(LocalNode);
            
            _Mesh = new WatsonMesh(_MeshSettings, _Self);

            if (_Topology == null || _Topology.Nodes.Count < 2)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "InitializeMeshNetworks fewer than two nodes exists, exiting");
                return;
            }
            else
            { 
                foreach (Node currNode in _Topology.Nodes)
                {
                    if (currNode.NodeId == LocalNode.NodeId) continue;
                    Peer currPeer = BuildPeerFromNode(currNode);
                    _Logging.Log(LoggingModule.Severity.Info, "InitializeMeshNetwork adding peer " + currNode.ToString());
                    _Mesh.Add(currPeer);
                }
            }

            _Mesh.PeerConnected = MeshPeerConnected;
            _Mesh.PeerDisconnected = MeshPeerDisconnected;
            _Mesh.AsyncMessageReceived = MeshAsyncMessageReceived;
            _Mesh.SyncMessageReceived = MeshSyncMessageReceived;
            _Mesh.StartServer(); 
        }

        private Peer BuildPeerFromNode(Node node)
        {
            Peer peer = null;

            if (node.Tcp.Ssl)
            {
                if (!String.IsNullOrEmpty(node.Tcp.PfxCertificateFile) && !String.IsNullOrEmpty(node.Tcp.PfxCertificatePass))
                {
                    peer = new Peer(
                        node.Tcp.IpAddress, 
                        node.Tcp.Port, 
                        true, 
                        node.Tcp.PfxCertificateFile, 
                        node.Tcp.PfxCertificatePass);
                }
                else
                {
                    peer = new Peer(
                        node.Tcp.IpAddress,
                        node.Tcp.Port, 
                        true);
                }
            }
            else
            {
                peer = new Peer(
                    node.Tcp.IpAddress,
                    node.Tcp.Port, 
                    false);
            }

            return peer;
        }

        #endregion

        #region Private-Mesh-Networking-Callbacks

        private bool MeshAsyncMessageReceived(Peer peer, byte[] data)
        {
            #region Check-for-Null-Values

            if (peer == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MeshAsyncMessageReceived message received without peer defined");
                return false;
            }

            if (data == null || data.Length < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MeshAsyncMessageReceived no data received in message from peer " + peer.ToString());
                return false;
            }

            #endregion

            #region Deserialize

            Message msg = null;
             
            try
            {
                msg = Common.DeserializeJson<Message>(data);
            }
            catch (Exception)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MeshAsyncMessageReceived unable to deserialize message data");
                return false;
            }

            #endregion

            #region Debug-Enumerate

            if (_Settings.Topology.DebugMeshNetworking)
            {
                int dataLen = 0;
                if (msg.Data != null && msg.Data.Length > 0) dataLen = msg.Data.Length;
                _Logging.Log(LoggingModule.Severity.Info, "MeshAsyncMessageReceived from node ID " + msg.From.NodeId + ": " + dataLen + " bytes");
            }

            if (_Settings.Topology.DebugMessages)
            {
                _Logging.Log(LoggingModule.Severity.Info,
                    "MeshAsyncMessageReceived received: " +
                    Environment.NewLine +
                    Common.SerializeJson(msg, true));
            }

            #endregion

            return _MessageMgr.ProcessAsyncMessage(msg);
        }

        private byte[] MeshSyncMessageReceived(Peer peer, byte[] data)
        {
            #region Check-for-Null-Values

            if (peer == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MeshSyncMessageReceived message received without peer defined");
                return null;
            }

            if (data == null || data.Length < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MeshSyncMessageReceived no data received in message from peer " + peer.ToString());
                return null;
            }

            #endregion

            #region Deserialize

            Message msg = null;

            try
            {
                msg = Common.DeserializeJson<Message>(data);
            }
            catch (Exception)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MeshSyncMessageReceived unable to deserialize message data");
                return null;
            }

            #endregion

            #region Debug-Enumerate

            if (_Settings.Topology.DebugMeshNetworking)
            {
                int dataLen = 0;
                if (msg.Data != null && msg.Data.Length > 0) dataLen = msg.Data.Length;
                _Logging.Log(LoggingModule.Severity.Info, "MeshSyncMessageReceived from node ID " + msg.From.NodeId + " [" + msg.Type.ToString() + "]: " + dataLen + " bytes");
            }

            if (_Settings.Topology.DebugMessages)
            {
                _Logging.Log(LoggingModule.Severity.Info,
                    "MeshSyncMessageReceived received: " +
                    Environment.NewLine +
                    Common.SerializeJson(msg, true));
            }

            #endregion

            #region Process

            Message resp = _MessageMgr.ProcessSyncMessage(msg);
            if (resp != null)
            {
                _Logging.Log(LoggingModule.Severity.Info, "MeshSyncMessageReceived responding to node ID " + msg.From.NodeId + " [" + msg.Type.ToString() + "]: " + resp.Success);
                return Encoding.UTF8.GetBytes(Common.SerializeJson(resp, false));
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MeshSyncMessageReceived unable to retrieve response message");
                return null;
            }

            #endregion 
        }

        private bool MeshPeerConnected(Peer peer)
        {
            _Logging.Log(LoggingModule.Severity.Info, "MeshPeerConnected " + peer.ToString());
            return true;
        }

        private bool MeshPeerDisconnected(Peer peer)
        {
            _Logging.Log(LoggingModule.Severity.Info, "MeshPeerDisconnected " + peer.ToString());
            return true;
        }

        #endregion
    }
}
