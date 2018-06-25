using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    /// <summary>
    /// Handles outgoing message requests.
    /// </summary>
    public class OutboundMessageHandler
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging; 
        private TopologyManager _Topology;
        private TaskManager _Tasks;

        #endregion

        #region Constructors-and-Factories

        public OutboundMessageHandler(
            Settings settings, 
            LoggingModule logging, 
            TopologyManager topology,
            TaskManager tasks)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging)); 
            if (topology == null) throw new ArgumentNullException(nameof(topology));
            if (tasks == null) throw new ArgumentNullException(nameof(tasks));

            _Settings = settings;
            _Logging = logging; 
            _Topology = topology;
            _Tasks = tasks;
        }

        #endregion

        #region Public-Methods

        #region Container-Methods

        public bool ContainerList(RequestMetadata md, Node node, out List<ContainerSettings> containers)
        {
            containers = new List<ContainerSettings>(); 

            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ContainerList, null, md.ToBytes());
            Message msgIn = _Topology.SendSyncMessage(msgOut);
            if (msgIn == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerList unable to retrieve response from node ID " + node.NodeId);
                return false;
            }
             
            if (msgIn.Success != null && Common.IsTrue(msgIn.Success))
            {
                try
                {
                    containers = Common.DeserializeJson<List<ContainerSettings>>(msgIn.Data);
                    _Logging.Log(LoggingModule.Severity.Debug, "ContainerList response includes " + containers.Count + " containers from node ID " + node.NodeId);
                    return true;
                }
                catch (Exception e)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerList unable to process response from node ID " + node.NodeId + ": " + e.Message);
                    return false;
                }
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerList failure reported on node ID " + node.NodeId);
                return false;
            }
        }

        public bool ContainerEnumerate(RequestMetadata md, Node node, out ContainerMetadata metadata)
        {
            metadata = new ContainerMetadata();
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ContainerEnumerate, null, md.ToBytes());
            Message msgIn = _Topology.SendSyncMessage(msgOut);
            if (msgIn == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerEnumerate unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                return false;
            }

            if (msgIn.Success != null && Common.IsTrue(msgIn.Success))
            {
                try
                {
                    metadata = Common.DeserializeJson<ContainerMetadata>(msgIn.Data);
                    _Logging.Log(LoggingModule.Severity.Debug, "ContainerEnumerate response includes " + metadata.Objects.Count + " objects in " + metadata.User + "/" + metadata.Name + " from node ID " + node.NodeId);
                    return true;
                }
                catch (Exception e)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerEnumerate unable to process response from node ID " + node.NodeId + ": " + e.Message);
                    return false;
                }
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerEnumerate failure reported on node ID " + node.NodeId);
                return false;
            }
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

        public bool ContainerCreate(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ContainerCreate replication mode set to none");
                return true;
            }

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ContainerCreate no replicas found in topology");
                return true;   
            }

            md.Http.Data = Encoding.UTF8.GetBytes(Common.SerializeJson(settings, false));

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ContainerCreateInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerCreate unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ContainerDelete(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ContainerDelete replication mode set to none");
                return true;
            }

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ContainerDelete no replicas found in topologyy");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ContainerDeleteInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ContainerUpdate(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ContainerUpdate replication mode set to none");
                return true;
            }

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ContainerUpdate no replicas found in topology");
                return true;
            }

            md.Http.Data = Encoding.UTF8.GetBytes(Common.SerializeJson(settings, false));

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ContainerUpdateInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerUpdate unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ContainerExists(RequestMetadata md, Node node)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ContainerExists, null, md.ToBytes());
            Message msgIn = _Topology.SendSyncMessage(msgOut);
            if (msgIn == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerExists unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                return false;
            }
             
            _Logging.Log(LoggingModule.Severity.Info, "ContainerExists response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);

            return Common.IsTrue(msgIn.Success);
        }

        public bool ContainerClearAuditLog(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ContainerClearAuditLog replication mode set to none");
                return true;
            }

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ContainerClearAuditLog no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ContainerClearAuditLogInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerClearAuditLog unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        #endregion

        #region Object-Methods

        public bool ObjectCreate(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ObjectCreate replication mode set to none");
                return true;
            }

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ObjectCreate no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ObjectCreateInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectCreate unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ObjectDelete(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ObjectDelete replication mode set to none");
                return true;
            }

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ObjectDelete no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ObjectDeleteInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete unable to replicate to " + currNode.ToString());
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
                _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete unable to replicate to node ID " + node.NodeId);
                return false;
            } 

            return true;
        }

        public bool ObjectWriteRange(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ObjectWriteRange replication mode set to none");
                return true;
            }

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ObjectWriteRange no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ObjectWriteRangeInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteRange unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ObjectRename(RequestMetadata md, ContainerSettings settings)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Replication == ReplicationMode.None)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ObjectRename replication mode set to none");
                return true;
            }

            List<Node> nodes = _Topology.GetReplicas();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ObjectRename no replicas found in topology");
                return true;
            }

            bool success = true;

            foreach (Node currNode in nodes)
            {
                if (!ObjectRenameInternal(md, currNode, settings.Replication))
                {
                    success = false;
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unable to replicate to " + currNode.ToString());
                }
            }

            return success;
        }

        public bool ObjectExists(RequestMetadata md, Node node)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectExists, null, md.ToBytes());
            Message msgIn = _Topology.SendSyncMessage(msgOut);
            if (msgIn == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ObjectExists unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                return false;
            }

            _Logging.Log(LoggingModule.Severity.Info, "ObjectExists response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

            return Common.IsTrue(msgIn.Success);
        }

        public bool ObjectRead(RequestMetadata md, Node node, out byte[] data)
        {
            data = null;

            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectRead, null, md.ToBytes());
            Message msgIn = _Topology.SendSyncMessage(msgOut);
            if (msgIn == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                return false;
            }
             
            if (msgIn.Success != null && Common.IsTrue(msgIn.Success))
            {
                if (msgIn.Data != null && msgIn.Data.Length > 0)
                {
                    data = new byte[msgIn.Data.Length];
                    Buffer.BlockCopy(msgIn.Data, 0, data, 0, msgIn.Data.Length);
                    _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead response length " + data.Length + " bytes for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return true;
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead request success but no data returned for for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return true;
                }
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead failure reported on node ID " + node.NodeId);
                return false;
            }
        }

        public bool ObjectMetadata(RequestMetadata md, Node node, out ObjectMetadata metadata)
        {
            metadata = null;
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ObjectMetadata, null, md.ToBytes());
            Message msgIn = _Topology.SendSyncMessage(msgOut);

            if (msgIn == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ObjectMetadata unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                return false;
            }

            if (msgIn.Success != null && Common.IsTrue(msgIn.Success))
            {
                try
                {
                    metadata = Common.DeserializeJson<ObjectMetadata>(msgIn.Data);
                    _Logging.Log(LoggingModule.Severity.Debug, "ObjectMetadata retrieved for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return true;
                }
                catch (Exception e)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectMetadata unable to process response from node ID " + node.NodeId + ": " + e.Message);
                    return false;
                }
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ObjectMetadata failure reported on node ID " + node.NodeId);
                return false;
            }
        }

        #endregion

        #region URL-Methods

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

            return new HttpResponse(md.Http, true, status, headers, null, null, true);
        }

        #endregion

        #endregion

        #region Private-Methods

        #region Container-Methods

        private bool ContainerCreateInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ReplicationContainerCreate, null, md.ToBytes());

            bool success = false;
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSyncMessage(msgOut);
                if (msgIn == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerCreateInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                    return false;
                }
                 
                _Logging.Log(LoggingModule.Severity.Info, "ContainerCreateInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                success = _Topology.SendAsyncMessage(msgOut);
                if (!success)
                {
                    _Logging.Log(LoggingModule.Severity.Info, "ContainerCreateInternal unable to send message to node ID " + node.NodeId + ", queuing");

                    TaskObject currTask = new TaskObject(TaskType.Message, node.NodeId, msgOut, null);
                    if (!_Tasks.Add(currTask))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerCreateInternal unable to queue task to node ID " + node.NodeId);
                        return false;
                    }
                }

                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ContainerDeleteInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ReplicationContainerDelete, null, md.ToBytes());

            bool success = false;
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSyncMessage(msgOut);
                if (msgIn == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerDeleteInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                    return false;
                }
                 
                _Logging.Log(LoggingModule.Severity.Info, "ContainerDeleteInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                success = _Topology.SendAsyncMessage(msgOut);
                if (!success)
                {
                    _Logging.Log(LoggingModule.Severity.Info, "ContainerDeleteInternal unable to send message to node ID " + node.NodeId + ", queuing");

                    TaskObject currTask = new TaskObject(TaskType.Message, node.NodeId, msgOut, null);
                    if (!_Tasks.Add(currTask))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerDeleteInternal unable to queue task to node ID " + node.NodeId);
                        return false;
                    }
                }

                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ContainerUpdateInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ReplicationContainerUpdate, null, md.ToBytes());

            bool success = false;
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSyncMessage(msgOut);
                if (msgIn == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerUpdateInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                    return false;
                }
                 
                _Logging.Log(LoggingModule.Severity.Info, "ContainerUpdateInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                success = _Topology.SendAsyncMessage(msgOut);
                if (!success)
                {
                    _Logging.Log(LoggingModule.Severity.Info, "ContainerUpdateInternal unable to send message to node ID " + node.NodeId + ", queuing");

                    TaskObject currTask = new TaskObject(TaskType.Message, node.NodeId, msgOut, null);
                    if (!_Tasks.Add(currTask))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerUpdateInternal unable to queue task to node ID " + node.NodeId);
                        return false;
                    }
                }

                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ContainerClearAuditLogInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ReplicationContainerClearAuditLog, null, md.ToBytes());

            bool success = false;
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSyncMessage(msgOut);
                if (msgIn == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerClearAuditLogInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);
                    return false;
                }
                 
                _Logging.Log(LoggingModule.Severity.Info, "ContainerClearAuditLogInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                success = _Topology.SendAsyncMessage(msgOut);
                if (!success)
                {
                    _Logging.Log(LoggingModule.Severity.Info, "ContainerClearAuditLogInternal unable to send message to node ID " + node.NodeId + ", queuing");

                    TaskObject currTask = new TaskObject(TaskType.Message, node.NodeId, msgOut, null);
                    if (!_Tasks.Add(currTask))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerClearAuditLogInternal unable to queue task to node ID " + node.NodeId);
                        return false;
                    }
                }

                return true;
            }
            else
            {
                return true;
            }
        }

        #endregion

        #region Object-Methods

        private bool ObjectCreateInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ReplicationObjectCreate, null, md.ToBytes());

            bool success = false;
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSyncMessage(msgOut);
                if (msgIn == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectCreateInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return false;
                }
                
                _Logging.Log(LoggingModule.Severity.Info, "ObjectCreateInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                success = _Topology.SendAsyncMessage(msgOut);
                if (!success)
                {
                    _Logging.Log(LoggingModule.Severity.Info, "ObjectCreateInternal unable to send message to node ID " + node.NodeId + ", queuing");

                    TaskObject currTask = new TaskObject(TaskType.Message, node.NodeId, msgOut, null);
                    if (!_Tasks.Add(currTask))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectCreateInternal unable to queue task to node ID " + node.NodeId);
                        return false;
                    }
                }

                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ObjectDeleteInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ReplicationObjectDelete, null, md.ToBytes());

            bool success = false;
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSyncMessage(msgOut);
                if (msgIn == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectDeleteInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return false;
                }
                 
                _Logging.Log(LoggingModule.Severity.Info, "ObjectDeleteInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                success = _Topology.SendAsyncMessage(msgOut);
                if (!success)
                {
                    _Logging.Log(LoggingModule.Severity.Info, "ObjectDeleteInternal unable to send message to node ID " + node.NodeId + ", queuing");

                    TaskObject currTask = new TaskObject(TaskType.Message, node.NodeId, msgOut, null);
                    if (!_Tasks.Add(currTask))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectDeleteInternal unable to queue task to node ID " + node.NodeId);
                        return false;
                    }
                }

                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ObjectWriteRangeInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ReplicationObjectWriteRange, null, md.ToBytes());

            bool success = false;
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSyncMessage(msgOut);
                if (msgIn == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteRangeInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return false;
                }
                 
                _Logging.Log(LoggingModule.Severity.Info, "ObjectWriteRangeInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                success = _Topology.SendAsyncMessage(msgOut);
                if (!success)
                {
                    _Logging.Log(LoggingModule.Severity.Info, "ObjectWriteRangeInternal unable to send message to node ID " + node.NodeId + ", queuing");

                    TaskObject currTask = new TaskObject(TaskType.Message, node.NodeId, msgOut, null);
                    if (!_Tasks.Add(currTask))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteRangeInternal unable to queue task to node ID " + node.NodeId);
                        return false;
                    }
                }

                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ObjectRenameInternal(RequestMetadata md, Node node, ReplicationMode mode)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ReplicationObjectRename, null, md.ToBytes());

            bool success = false;
            if (mode == ReplicationMode.Sync)
            {
                Message msgIn = _Topology.SendSyncMessage(msgOut);
                if (msgIn == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectRenameInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                    return false;
                }
                 
                _Logging.Log(LoggingModule.Severity.Info, "ObjectRenameInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

                return Common.IsTrue(msgIn.Success);
            }
            else if (mode == ReplicationMode.Async)
            {
                success = _Topology.SendAsyncMessage(msgOut);
                if (!success)
                {
                    _Logging.Log(LoggingModule.Severity.Info, "ObjectRenameInternal unable to send message to node ID " + node.NodeId + ", queuing");

                    TaskObject currTask = new TaskObject(TaskType.Message, node.NodeId, msgOut, null);
                    if (!_Tasks.Add(currTask))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRenameInternal unable to queue task to node ID " + node.NodeId);
                        return false;
                    }
                }

                return true;
            }
            else
            {
                return true;
            }
        }

        private bool ObjectReadInternal(RequestMetadata md, Node node)
        {
            Message msgOut = new Message(_Topology.LocalNode, node, md.Sanitized(), MessageType.ReplicationObjectCreate, null, md.ToBytes());
            Message msgIn = _Topology.SendSyncMessage(msgOut);
            if (msgIn == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ObjectReadInternal unable to retrieve response for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);
                return false;
            }
             
            _Logging.Log(LoggingModule.Severity.Info, "ObjectReadInternal response " + msgIn.Success + " for " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " from node ID " + node.NodeId);

            return Common.IsTrue(msgIn.Success);
        }

        #endregion

        #endregion
    }
}
