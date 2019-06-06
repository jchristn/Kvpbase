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

using Kvpbase.Classes.Messaging;
using Kvpbase.Core; 

namespace Kvpbase.Classes.Managers
{
    /// <summary>
    /// Maintains and manages connectivity amongst nodes.  Relies on MessageManager to handle incoming messages.
    /// </summary>
    public class TopologyManager
    {
        #region Public-Members

        /// <summary>
        /// The local node.
        /// </summary>
        public Node LocalNode { get; private set; }

        /// <summary>
        /// Determine if the topology is empty or not.
        /// </summary>
        public bool IsEmpty
        {
            get
            {
                if (_Topology.Nodes == null || _Topology.Nodes.Count < 2) return true;
                return false;
            }
        }

        /// <summary>
        /// Determine if the mesh network is healthy.
        /// </summary>
        /// <returns>True if healthy.</returns>
        public bool IsNetworkHealthy
        {
            get
            {
                return _Mesh.IsHealthy();
            }
        }

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private Topology _Topology; 
        private UserManager _UserMgr; 
        private InboundHandler _InboundMessageHandler;

        private readonly object _TopologyLock;

        private MeshSettings _MeshSettings;
        private Peer _Self;
        private WatsonMesh _Mesh;

        #endregion

        #region Constructors-and-Factories

        public TopologyManager(Settings settings, LoggingModule logging, UserManager users, InboundHandler inboundMsgHandler)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging)); 
            if (users == null) throw new ArgumentNullException(nameof(users)); 
            if (inboundMsgHandler == null) throw new ArgumentNullException(nameof(inboundMsgHandler));

            _Settings = settings;
            _Logging = logging; 
            _UserMgr = users; 
            _InboundMessageHandler = inboundMsgHandler;

            _TopologyLock = new object();

            LoadTopologyFile();
            SetLocalNode();

            string error;
            if (!ValidateTopology(out error)) throw new Exception("Unable to validate topology: " + error);
             
            InitializeMeshNetwork(); 

            if (_Settings.Topology.DebugMeshNetworking) _Logging.Log(LoggingModule.Severity.Info, "TopologyManager debugging enabled, disable to reduce log verbocity"); 
        }

        #endregion

        #region Public-Methods
         
        /// <summary>
        /// Retrieve the list of nodes in the network.
        /// </summary>
        /// <returns>List of nodes.</returns>
        public List<Node> GetNodes()
        {
            lock (_TopologyLock)
            {
                return _Topology.Nodes;
            }
        }

        /// <summary>
        /// Retrieve the list of nodes that are considered replicas.
        /// </summary>
        /// <returns>List of nodes.</returns>
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
         
        /// <summary>
        /// Determine which node owns a particular user account.
        /// </summary>
        /// <param name="userGuid">User GUID.</param>
        /// <returns>Node.</returns>
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

        /// <summary>
        /// Retrieve a node by ID.
        /// </summary>
        /// <param name="nodeId">Node ID.</param>
        /// <returns>Node.</returns>
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
        
        /// <summary>
        /// Send a message asynchronously to another node.
        /// </summary>
        /// <param name="msgType">Type of message.</param>
        /// <param name="nodeId">Node ID.</param>
        /// <param name="data">Data to send.</param>
        /// <returns>True if successful.</returns>
        public bool SendAsync(RequestMetadata md, MessageType msgType, int nodeId, byte[] data)
        {
            Node rcpt = GetNodeById(nodeId);
            if (rcpt == null || rcpt == default(Node)) return false;

            MemoryStream ms = new MemoryStream();
            long contentLength = 0;
            if (data != null)
            {
                ms.Write(data, 0, data.Length);
                contentLength = data.Length;
                if (ms.CanSeek) ms.Seek(0, SeekOrigin.Begin);
            }

            Message msg = new Message(LocalNode, rcpt, md, msgType, null, contentLength, ms);
            return SendAsync(msg);
        }

        /// <summary>
        /// Send a message asynchronously to another node.
        /// </summary>
        /// <param name="msgType">Type of message.</param>
        /// <param name="nodeId">Node ID.</param>
        /// <param name="contentLength">Number of bytes contained in the stream.</param>
        /// <param name="stream">Stream containing data to send.</param>
        /// <returns>True if successful.</returns>
        public bool SendAsync(RequestMetadata md, MessageType msgType, int nodeId, long contentLength, Stream stream)
        {
            Node rcpt = GetNodeById(nodeId);
            if (rcpt == null || rcpt == default(Node)) return false;

            Message msg = new Message(LocalNode, rcpt, md, msgType, null, contentLength, stream);
            return SendAsync(msg);
        }

        /// <summary>
        /// Send a message asynchronously to another node.
        /// </summary>
        /// <param name="msg">Message.</param>
        /// <returns>True if successful.</returns>
        public bool SendAsync(Message msg)
        { 
            if (msg == null) throw new ArgumentNullException(nameof(msg));
            if (msg.To == null) throw new ArgumentException("Message does not contain 'To' node.");
             
            msg.From = LocalNode;
             
            byte[] msgHeaders = msg.ToHeaderBytes(); 

            long totalLen = msgHeaders.Length;
            MemoryStream ms = new MemoryStream();
            ms.Write(msgHeaders, 0, msgHeaders.Length);

            if (_Settings.Topology.DebugMessages) 
                _Logging.Log(LoggingModule.Severity.Debug, "SendAsyncMessage sending message:" + Environment.NewLine + msg.ToString()); 

            if (msg.ContentLength > 0 && msg.DataStream != null)
            {   
                if (msg.DataStream.CanSeek)
                    msg.DataStream.Seek(0, SeekOrigin.Begin);

                int bytesRead = 0;
                long bytesRemaining = msg.ContentLength;
                byte[] buffer = new byte[65536];

                while (bytesRemaining > 0)
                {
                    bytesRead = msg.DataStream.Read(buffer, 0, buffer.Length);
                    if (bytesRead > 0)
                    {
                        bytesRemaining -= bytesRead;
                        totalLen += bytesRead;
                        ms.Write(buffer, 0, bytesRead);
                    }
                }

                if (ms.CanSeek) ms.Seek(0, SeekOrigin.Begin); 
            }
             
            return _Mesh.SendAsync(
                msg.To.Tcp.IpAddress,
                msg.To.Tcp.Port,
                totalLen,
                ms);  
        }

        /// <summary>
        /// Send a message synchronously to another node.
        /// </summary>
        /// <param name="msgType">Type of message.</param>
        /// <param name="nodeId">Node ID.</param>
        /// <param name="data">Data to send.</param>
        /// <returns>True if successful.</returns>
        public Message SendSync(RequestMetadata md, MessageType msgType, int nodeId, byte[] data, int foo)
        {
            Node rcpt = GetNodeById(nodeId);
            if (rcpt == null || rcpt == default(Node)) return null;

            MemoryStream ms = new MemoryStream();
            long contentLength = 0;
            if (data != null)
            {
                ms.Write(data, 0, data.Length);
                contentLength = data.Length;
                if (ms.CanSeek) ms.Seek(0, SeekOrigin.Begin);
            }

            Message msg = new Message(LocalNode, rcpt, md, msgType, null, contentLength, ms);
            return SendSync(msg);
        }

        /// <summary>
        /// Send a message synchronously to another node.
        /// </summary>
        /// <param name="msgType">Type of message.</param>
        /// <param name="nodeId">Node ID.</param>
        /// <param name="contentLength">Number of data bytes in the stream.</param>
        /// <param name="stream">Stream containing data to send.</param>
        /// <returns>True if successful.</returns>
        public Message SendSync(RequestMetadata md, MessageType msgType, int nodeId, long contentLength, Stream stream)
        {
            Node rcpt = GetNodeById(nodeId);
            if (rcpt == null || rcpt == default(Node)) return null;

            Message msg = new Message(LocalNode, rcpt, md, msgType, null, contentLength, stream);
            return SendSync(msg);
        }

        /// <summary>
        /// Send a message synchronously to another node.
        /// </summary>
        /// <param name="msg">Message.</param>
        /// <returns>Message.</returns>
        public Message SendSync(Message msg)
        { 
            if (msg == null) throw new ArgumentNullException(nameof(msg)); 
            if (msg.To == null) throw new ArgumentException("Message does not contain 'To' node."); 
            msg.From = LocalNode;
             
            MemoryStream ms = new MemoryStream();
            MemoryStream tempMs = new MemoryStream();
            byte[] msgHeaders = msg.ToHeaderBytes(); 
            ms.Write(msgHeaders, 0, msgHeaders.Length);
            tempMs.Write(msgHeaders, 0, msgHeaders.Length);
            long totalLen = msgHeaders.Length; 

            if (_Settings.Topology.DebugMessages) 
                _Logging.Log(LoggingModule.Severity.Debug, "SendSyncMessage sending message:" + Environment.NewLine + msg.ToString()); 

            if (msg.ContentLength > 0)
            { 
                if (msg.DataStream.CanSeek)
                    msg.DataStream.Seek(0, SeekOrigin.Begin); 

                int bytesRead = 0;
                long bytesRemaining = msg.ContentLength;
                byte[] buffer = new byte[65536]; 

                while (bytesRemaining > 0)
                { 
                    bytesRead = msg.DataStream.Read(buffer, 0, buffer.Length);
                    if (bytesRead > 0)
                    {
                        bytesRemaining -= bytesRead;
                        totalLen += bytesRead;
                        ms.Write(buffer, 0, bytesRead);
                        tempMs.Write(buffer, 0, bytesRead);
                    }
                }  
            } 

            if (ms.CanSeek)
                ms.Seek(0, SeekOrigin.Begin);
            if (tempMs.CanSeek)
                tempMs.Seek(0, SeekOrigin.Begin);

            long responseLength = 0;
            Stream responseStream = null;
             
            if (!_Mesh.SendSync(
                msg.To.Tcp.IpAddress, 
                msg.To.Tcp.Port, 
                (1000 * CalculateTimeoutSeconds(msg.To, msg.ContentLength)), 
                totalLen, 
                ms, 
                out responseLength,
                out responseStream))
            { 
                _Logging.Log(LoggingModule.Severity.Warn, "SendSyncMessage [" + msg.To.Tcp.IpAddress + ":" + msg.To.Tcp.Port + "] unable to send message to node ID " + msg.To.NodeId);
                return null;
            } 

            responseStream.Seek(0, SeekOrigin.Begin);
            if (responseStream == null || !responseStream.CanRead)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSyncMessage [" + msg.To.Tcp.IpAddress + ":" + msg.To.Tcp.Port + "] no data returned from node ID " + msg.To.NodeId);
                return null;
            } 

            responseStream.Seek(0, SeekOrigin.Begin);
            Message resp = Message.FromStream(responseStream);
            return resp; 
        }
        
        /// <summary>
        /// Determine if a particular node is healthy.
        /// </summary>
        /// <param name="nodeId">Node ID.</param>
        /// <returns>True if healthy.</returns>
        public bool IsNodeHealthy(int nodeId)
        {
            Node currNode = GetNodeById(nodeId);
            return IsNodeHealthy(currNode);
        }

        /// <summary>
        /// Determine if a particular node is healthy.
        /// </summary>
        /// <param name="node">Node.</param>
        /// <returns>True if healthy.</returns>
        public bool IsNodeHealthy(Node node)
        {
            if (node == null) return false;
            if (node.Tcp.IpAddress.Equals(LocalNode.Tcp.IpAddress) && node.Tcp.Port.Equals(LocalNode.Tcp.Port)) return true;
            return _Mesh.IsHealthy(node.Tcp.IpAddress, node.Tcp.Port);
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

            if (_Topology.Replicas != null && _Topology.Replicas.Count > 0)
            {
                foreach (int currNodeId in _Topology.Replicas)
                {
                    if (!allNodeIds.Contains(currNodeId))
                    {
                        error = "Replica node ID " + currNodeId + " not found in node list.";
                        return false;
                    }
                }
            }

            #endregion

            return true;
        }
        
        private void InitializeMeshNetwork()
        {
            _MeshSettings = new MeshSettings();
            _MeshSettings.ReadDataStream = false;

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
                    _Mesh.Add(currPeer); 
                }
            }

            _Mesh.PeerConnected = MeshPeerConnected;
            _Mesh.PeerDisconnected = MeshPeerDisconnected; 
            _Mesh.AsyncStreamReceived = MeshAsyncStreamReceived; 
            _Mesh.SyncStreamReceived = MeshSyncStreamReceived;
            _Mesh.Start();
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

        private int CalculateTimeoutSeconds(Node node, long contentLength)
        { 
            long numSeconds = (contentLength / node.Tcp.Timeout.ExpectedXferRateBytesPerSec) * 2;
            if (numSeconds <= node.Tcp.Timeout.MinTimeoutSec) return node.Tcp.Timeout.MinTimeoutSec;
            if (numSeconds >= node.Tcp.Timeout.MaxTimeoutSec) return node.Tcp.Timeout.MaxTimeoutSec;
            return (int)numSeconds;
        }
         
        #endregion

        #region Private-Mesh-Networking-Callbacks
         
        private bool MeshAsyncStreamReceived(Peer peer, long contentLength, Stream stream)
        {
            if (peer == null) return false;
            if (stream == null || !stream.CanRead || contentLength <= 0) return false;
            Message msg = Message.FromStream(stream); 
            return _InboundMessageHandler.ProcessAsyncStream(msg);
        }
         
        private SyncResponse MeshSyncStreamReceived(Peer peer, long contentLength, Stream stream)
        {
            if (peer == null) return null;
            if (stream == null || !stream.CanRead || contentLength <= 0) return null;
            Message msg = Message.FromStream(stream);
            Message respMsg = _InboundMessageHandler.ProcessSyncStream(msg);
             
            if (respMsg != null)
            {
                MemoryStream ms = new MemoryStream();
                MemoryStream tempMs = new MemoryStream();
                byte[] headers = respMsg.ToHeaderBytes(); 

                long totalLen = headers.Length;
                ms.Write(headers, 0, headers.Length);
                tempMs.Write(headers, 0, headers.Length);

                if (respMsg.ContentLength > 0)
                {
                    totalLen += respMsg.ContentLength; 

                    int bytesRead = 0;
                    long bytesRemaining = respMsg.ContentLength;
                    byte[] buffer = new byte[65536];

                    while (bytesRemaining > 0)
                    {
                        bytesRead = respMsg.DataStream.Read(buffer, 0, buffer.Length);
                        if (bytesRead > 0)
                        { 
                            bytesRemaining -= bytesRead;
                            ms.Write(buffer, 0, bytesRead);
                            tempMs.Write(buffer, 0, bytesRead);
                        }
                    }
                }

                ms.Seek(0, SeekOrigin.Begin);
                tempMs.Seek(0, SeekOrigin.Begin); 

                return new SyncResponse(totalLen, ms);
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MeshSyncStreamReceived unable to retrieve response message");
                return null;
            } 
        }

        private bool MeshPeerConnected(Peer peer)
        { 
            return true;
        }

        private bool MeshPeerDisconnected(Peer peer)
        {
            _Logging.Log(LoggingModule.Severity.Warn, "MeshPeerDisconnected " + peer.ToString());
            return true;
        }

        #endregion
    }
}
