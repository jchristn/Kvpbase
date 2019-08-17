using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Classes;
using Kvpbase.Classes.Managers;
using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase.Classes.Messaging
{
    /// <summary>
    /// Handles outgoing message requests.
    /// </summary>
    public class OutboundHandler
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private TopologyManager _Topology; 

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="settings">Settings.</param>
        /// <param name="logging">LoggingModule instance.</param>
        /// <param name="topology">TopologyManager instance.</param> 
        public OutboundHandler(Settings settings, LoggingModule logging, TopologyManager topology)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (topology == null) throw new ArgumentNullException(nameof(topology)); 

            _Settings = settings;
            _Logging = logging;
            _Topology = topology; 
        }

        #endregion

        #region Public-URL-Methods

        public string BuildRedirectUrl(RequestMetadata md, Node node)
        {
            string ret = "";

            if (node.Http.Ssl) ret += "https://";
            else ret += "http://";
            ret += node.Http.DnsHostname + ":" + node.Http.Port;
            ret += md.Http.RawUrlWithQuery;

            return ret;
        }

        public HttpResponse BuildRedirectResponse(RequestMetadata md, Node node, out string redirectUrl)
        {
            redirectUrl = BuildRedirectUrl(md, node);
            Dictionary<string, string> headers = new Dictionary<string, string>();
            headers.Add("Location", BuildRedirectUrl(md, node));

            int status = 301;
            if (_Settings.Redirection.Mode == RedirectMode.MovedPermanently) status = 301;
            else if (_Settings.Redirection.Mode == RedirectMode.Found) status = 302;
            else if (_Settings.Redirection.Mode == RedirectMode.TemporaryRedirect) status = 307;
            else if (_Settings.Redirection.Mode == RedirectMode.PermanentRedirect) status = 308;

            return new HttpResponse(md.Http, status, headers);
        }

        #endregion

        #region Public-Container-Methods

        public bool ContainerList(RequestMetadata md, Node node, out List<ContainerSettings> containers)
        {
            containers = new List<ContainerSettings>();
             
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ContainerList, null, 0, null); 
            Message msgIn = _Topology.SendSync(msgOut);
            if (msgIn == null)
            {
                _Logging.Warn("ContainerList unable to retrieve response from node ID " + node.NodeId);
                return false;
            } 

            if (msgIn.Success != null && Common.IsTrue(msgIn.Success))
            {
                try
                { 
                    msgIn.DataStream.Seek(0, SeekOrigin.Begin);
                    containers = Common.DeserializeJson<List<ContainerSettings>>(Common.StreamToBytes(msgIn.DataStream));
                    _Logging.Debug("ContainerList response includes " + containers.Count + " containers from node ID " + node.NodeId);
                    return true;
                }
                catch (Exception e)
                {
                    _Logging.Warn("ContainerList unable to process response from node ID " + node.NodeId + ": " + e.Message);
                    return false;
                }
            }
            else
            {
                _Logging.Warn("ContainerList failure reported on node ID " + node.NodeId);
                return false;
            }
        }

        public bool ContainerEnumerate(RequestMetadata md, Node node, out ContainerMetadata metadata)
        {
            metadata = new ContainerMetadata();
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ContainerEnumerate, null, 0, null);
            Message msgIn = _Topology.SendSync(msgOut);
            if (msgIn == null)
            {
                _Logging.Warn("ContainerEnumerate unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                return false;
            }

            if (msgIn.Success != null && Common.IsTrue(msgIn.Success))
            {
                try
                {
                    metadata = Common.DeserializeJson<ContainerMetadata>(Common.StreamToBytes(msgIn.DataStream));
                    _Logging.Debug("ContainerEnumerate response includes " + metadata.Objects.Count + " objects in " + metadata.User + "/" + metadata.Name + " from node ID " + node.NodeId);
                    return true;
                }
                catch (Exception e)
                {
                    _Logging.Warn("ContainerEnumerate unable to process response from node ID " + node.NodeId + ": " + e.Message);
                    return false;
                }
            }
            else
            {
                _Logging.Warn("ContainerEnumerate failure reported on node ID " + node.NodeId);
                return false;
            }
        }

        public bool ContainerCreate(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None) return true;

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Debug("ContainerCreate no replicas found in topology");
                return true;
            }

            md.Http.Data = Encoding.UTF8.GetBytes(Common.SerializeJson(settings, false));

            bool success = true;

            foreach (Node currNode in nodes)
            { 
                if (!ContainerCreateInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Warn("ContainerCreate unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ContainerDelete(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None) return true;

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Debug("ContainerDelete no replicas found in topologyy");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ContainerDeleteInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Warn("ContainerDelete unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ContainerUpdate(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None) return true;

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Debug("ContainerUpdate no replicas found in topology");
                return true;
            }

            md.Http.Data = Encoding.UTF8.GetBytes(Common.SerializeJson(settings, false));

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ContainerUpdateInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Warn("ContainerUpdate unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ContainerExists(RequestMetadata md, Node node)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ContainerExists, null, 0, null);
            Message msgIn = _Topology.SendSync(msgOut);
            if (msgIn == null)
            {
                _Logging.Warn("ContainerExists unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                return false;
            }

            _Logging.Info("ContainerExists response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);

            return Common.IsTrue(msgIn.Success);
        }

        public bool ContainerClearAuditLog(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None) return true;

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Debug("ContainerClearAuditLog no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ContainerClearAuditLogInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Warn("ContainerClearAuditLog unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool FindContainerOwners(RequestMetadata md, out List<Node> nodes)
        {
            nodes = new List<Node>();
            List<Node> allNodes = _Topology.GetNodes();
            if (allNodes == null || allNodes.Count < 2) return false;

            foreach (Node currNode in allNodes)
            {
                if (_Topology.IsNodeHealthy(currNode.NodeId))
                {
                    if (ContainerExists(md, currNode)) nodes.Add(currNode.Sanitized());
                }
            }

            if (nodes.Count > 0) return true;
            return false;
        }

        #endregion

        #region Public-Object-Methods

        public bool ObjectCreate(RequestMetadata md, ContainerSettings settings, long contentLength, Stream stream)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new ArgumentException("Cannot read from supplied stream.");
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None) return true;

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Debug("ObjectCreate no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ObjectCreateInternal(md, currNode, settings.Replication, contentLength, stream))
                {
                    success = false;
                    _Logging.Warn("ObjectCreate unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ObjectDelete(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None) return true;

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Debug("ObjectDelete no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ObjectDeleteInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Warn("ObjectDelete unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ObjectDelete(RequestMetadata md, Node node)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (node == null) throw new ArgumentNullException(nameof(node));

            if (!ObjectDeleteInternal(md, node, ReplicationMode.Sync))
            {
                _Logging.Warn("ObjectDelete unable to replicate to node ID " + node.NodeId);
                return false;
            }

            return true;
        }

        public bool ObjectWriteRange(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None) return true;

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Debug("ObjectWriteRange no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ObjectWriteRangeInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Warn("ObjectWriteRange unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ObjectWriteTags(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None) return true;

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Debug("ObjectWriteTags no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ObjectWriteTagsInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Warn("ObjectWriteTags unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ObjectRename(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None) return true;

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Debug("ObjectRename no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ObjectRenameInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Warn("ObjectRename unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ObjectExists(RequestMetadata md, Node node)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectExists, null, 0, null);
            Message msgIn = _Topology.SendSync(msgOut);
            if (msgIn == null)
            {
                _Logging.Warn("ObjectExists unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                return false;
            }

            _Logging.Info("ObjectExists response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

            return Common.IsTrue(msgIn.Success);
        }

        public bool ObjectRead(RequestMetadata md, Node node, out long contentLength, out Stream stream)
        {
            contentLength = 0;
            stream = null;

            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectRead, null, 0, null);
            Message msgIn = _Topology.SendSync(msgOut);
            if (msgIn == null)
            {
                _Logging.Warn("ObjectRead unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                return false;
            }

            if (msgIn.Success != null && Common.IsTrue(msgIn.Success))
            {
                if (msgIn.ContentLength > 0)
                {
                    contentLength = msgIn.ContentLength;
                    stream = msgIn.DataStream;
                    _Logging.Debug("ObjectRead response length " + contentLength + " bytes for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return true;
                }
                else
                {
                    _Logging.Debug("ObjectRead request success but no data returned for for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return true;
                }
            }
            else
            {
                _Logging.Warn("ObjectRead failure reported on node ID " + node.NodeId);
                return false;
            }
        }

        public bool ObjectMetadata(RequestMetadata md, Node node, out ObjectMetadata metadata)
        {
            metadata = null;
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectMetadata, null, 0, null);
            Message msgIn = _Topology.SendSync(msgOut);

            if (msgIn == null)
            {
                _Logging.Warn("ObjectMetadata unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                return false;
            }

            if (msgIn.Success != null && Common.IsTrue(msgIn.Success))
            {
                try
                {
                    metadata = Common.DeserializeJson<ObjectMetadata>(Common.StreamToBytes(msgIn.DataStream));
                    _Logging.Debug("ObjectMetadata retrieved for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return true;
                }
                catch (Exception e)
                {
                    _Logging.Warn("ObjectMetadata unable to process response from node ID " + node.NodeId + ": " + e.Message);
                    return false;
                }
            }
            else
            {
                _Logging.Warn("ObjectMetadata failure reported on node ID " + node.NodeId);
                return false;
            }
        }

        #endregion

        #region Private-Container-Methods

        private bool ContainerCreateInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            long contentLength = 0;
            MemoryStream ms = new MemoryStream();
            if (md.Http.Data != null && md.Http.Data.Length > 0)
            {
                ms.Write(md.Http.Data, 0, md.Http.Data.Length);
                contentLength = md.Http.Data.Length;
            }
            ms.Seek(0, SeekOrigin.Begin);

            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ContainerCreate, null, contentLength, ms);
             
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSync(msgOut);
                if (msgIn == null)
                {
                    _Logging.Warn("ContainerCreateInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                    return false;
                }

                _Logging.Info("ContainerCreateInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                _Topology.SendAsync(msgOut);
                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ContainerDeleteInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ContainerDelete, null, 0, null);
             
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSync(msgOut);
                if (msgIn == null)
                {
                    _Logging.Warn("ContainerDeleteInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                    return false;
                }

                _Logging.Info("ContainerDeleteInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                _Topology.SendAsync(msgOut);
                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ContainerUpdateInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            long contentLength = 0;
            MemoryStream ms = new MemoryStream();
            if (md.Http.Data != null && md.Http.Data.Length > 0)
            {
                ms.Write(md.Http.Data, 0, md.Http.Data.Length);
                contentLength = md.Http.Data.Length;
            }
            ms.Seek(0, SeekOrigin.Begin);

            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ContainerUpdate, null, contentLength, ms);
             
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSync(msgOut);
                if (msgIn == null)
                {
                    _Logging.Warn("ContainerUpdateInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                    return false;
                }

                _Logging.Debug("ContainerUpdateInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                _Topology.SendAsync(msgOut);
                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ContainerClearAuditLogInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ContainerClearAuditLog, null, 0, null);
             
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSync(msgOut);
                if (msgIn == null)
                {
                    _Logging.Warn("ContainerClearAuditLogInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                    return false;
                }

                _Logging.Info("ContainerClearAuditLogInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                _Topology.SendAsync(msgOut);
                return true;
            }
            else
            {
                return true;
            }
        }

        #endregion

        #region Private-Object-Methods

        private bool ObjectCreateInternal(RequestMetadata md, Node node, ReplicationMode mode, long contentLength, Stream stream)
        {
            stream.Seek(0, SeekOrigin.Begin);
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectCreate, null, contentLength, stream);
             
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSync(msgOut);
                if (msgIn == null)
                {
                    _Logging.Warn("ObjectCreateInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return false;
                }

                _Logging.Info("ObjectCreateInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                _Topology.SendAsync(msgOut);
                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ObjectDeleteInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectDelete, null, 0, null);
             
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSync(msgOut);
                if (msgIn == null)
                {
                    _Logging.Warn("ObjectDeleteInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return false;
                }

                _Logging.Info("ObjectDeleteInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                _Topology.SendAsync(msgOut);
                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ObjectWriteRangeInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectWriteRange, null, md.Http.ContentLength, md.Http.DataStream);
             
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSync(msgOut);
                if (msgIn == null)
                {
                    _Logging.Warn("ObjectWriteRangeInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return false;
                }

                _Logging.Info("ObjectWriteRangeInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                _Topology.SendAsync(msgOut);
                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ObjectWriteTagsInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectWriteTags, null, 0, null);
             
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSync(msgOut);
                if (msgIn == null)
                {
                    _Logging.Warn("ObjectWriteTagsInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return false;
                }

                _Logging.Info("ObjectWriteTagsInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                _Topology.SendAsync(msgOut);
                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ObjectRenameInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectRename, null, 0, null);
             
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSync(msgOut);
                if (msgIn == null)
                {
                    _Logging.Warn("ObjectRenameInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return false;
                }

                _Logging.Info("ObjectRenameInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                _Topology.SendAsync(msgOut);
                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ObjectReadInternal(RequestMetadata md, Node node)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectCreate, null, 0, null);
            Message msgIn = _Topology.SendSync(msgOut);
            if (msgIn == null)
            {
                _Logging.Warn("ObjectReadInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                return false;
            }

            _Logging.Info("ObjectReadInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

            return Common.IsTrue(msgIn.Success);
        }

        #endregion
    }
}
