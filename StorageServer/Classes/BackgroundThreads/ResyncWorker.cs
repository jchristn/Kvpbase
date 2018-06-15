using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public class ResyncWorker
    {
        #region Public-Members

        public string WorkerGuid { get; private set; }
        public Node SourceNode { get; private set; }
        public string UserName { get; private set; }
        public string ContainerName { get; private set; }
        public DateTime? StartTime { get; private set; }
        public DateTime? EndTime { get; private set; }
        public double TotalMilliseconds { get; private set; }

        public List<ContainerSettings> Containers { get; private set; }

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private TopologyManager _Topology;
        private OutboundMessageHandler _OutboundReplicationMgr;
        private ContainerManager _ContainerMgr;
        private ContainerHandler _Containers;
        private ObjectHandler _Objects;

        private bool _Running = false;

        private CancellationTokenSource _TokenSource;
        private CancellationToken _Token;

        private Func<string, bool> _TaskComplete;

        #endregion

        #region Constructors-and-Factories

        public ResyncWorker(
            Settings settings,
            LoggingModule logging,
            TopologyManager topology,
            OutboundMessageHandler replication,
            ContainerManager containerMgr,
            ContainerHandler containers,
            ObjectHandler objects,
            Node node,
            string userName,
            string containerName,
            DateTime? startTime,
            string guid,
            Func<string, bool> taskComplete)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (topology == null) throw new ArgumentNullException(nameof(topology));
            if (replication == null) throw new ArgumentNullException(nameof(replication));
            if (containerMgr == null) throw new ArgumentNullException(nameof(containerMgr));
            if (containers == null) throw new ArgumentNullException(nameof(containers));
            if (objects == null) throw new ArgumentNullException(nameof(objects));
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (taskComplete == null) throw new ArgumentNullException(nameof(taskComplete));

            _Settings = settings;
            _Logging = logging;
            _Topology = topology;
            _OutboundReplicationMgr = replication;
            _ContainerMgr = containerMgr;
            _Containers = containers;
            _Objects = objects;
            _TaskComplete = taskComplete;

            SourceNode = node;

            UserName = userName;
            ContainerName = containerName;
            StartTime = startTime;
            WorkerGuid = guid;

            Containers = new List<ContainerSettings>();

            Node testNode = _Topology.GetNodeById(SourceNode.NodeId);
            if (testNode == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker unable to verify node ID " + SourceNode.NodeId + " in topology");
                throw new ArgumentException("Node does not exist in topology.");
            }

            _TokenSource = new CancellationTokenSource();
            _Token = _TokenSource.Token;
        }

        #endregion

        #region Public-Methods

        public bool Start()
        {
            if (_Running)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker Start already running for node " + SourceNode.ToString());
                return false;
            }

            _Running = true;
            StartTime = DateTime.Now.ToUniversalTime();

            Task.Run(() => BackgroundTask(), _Token);
            return true;
        }

        public void Stop()
        {
            _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker " + WorkerGuid + " cancellation requested");
            _TokenSource.Cancel();
        }

        #endregion

        #region Private-Methods

        private void BackgroundTask()
        {
            try
            {
                #region Get-Containers

                List<ContainerSettings> containers = new List<ContainerSettings>();
                List<ContainerSettings> filtered = new List<ContainerSettings>();

                RequestMetadata md = BuildMetadata(null, null, UserName, ContainerName, null, "get");
                if (!_OutboundReplicationMgr.ContainerList(md, SourceNode, out containers))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask unable to retrieve container list from " + SourceNode.ToString());
                    return;
                }
                 
                if (containers == null || containers.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask no containers found on node " + SourceNode.ToString());
                    return;
                }

                if (!String.IsNullOrEmpty(UserName) && !String.IsNullOrEmpty(ContainerName))
                {
                    // Filter list
                    foreach (ContainerSettings currContainer in containers)
                    {
                        if (currContainer.User.ToLower().Equals(UserName)
                            && currContainer.Name.ToLower().Equals(ContainerName))
                        {
                            filtered.Add(currContainer);
                        } 
                    }

                    if (filtered == null || filtered.Count < 1)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask no matching containers found on node " + SourceNode.ToString());
                        return;
                    }

                    Containers = new List<ContainerSettings>(filtered);
                }
                else
                {
                    Containers = new List<ContainerSettings>(containers);
                }
                 
                #endregion

                #region Process-Each-Container

                foreach (ContainerSettings currContainer in Containers)
                {
                    #region Pre-Enumerate

                    _Logging.Log(LoggingModule.Severity.Info, "ResyncWorker BackgroundTask " + WorkerGuid + " starting container " + currContainer.User + "/" + currContainer.Name + " on node ID " + SourceNode.NodeId);

                    #endregion

                    #region Variables

                    int? currIndex = 0;
                    md = BuildMetadata(currIndex, null, currContainer.User, currContainer.Name, null, "get");
                    ContainerMetadata currMetadata = new ContainerMetadata();
                    bool running = true;
                    Container container = null;
                    ErrorCode error;

                    #endregion

                    #region Retrieve-or-Create-Container

                    if (!_ContainerMgr.Exists(currContainer.User, currContainer.Name))
                    {
                        _Logging.Log(LoggingModule.Severity.Info, "ResyncWorker BackgroundTask " + WorkerGuid + " creating container " + currContainer.User + "/" + currContainer.Name);
                        _ContainerMgr.Add(currContainer);
                    }
                    
                    if (!_ContainerMgr.GetContainer(currContainer.User, currContainer.Name, out container))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " unable to retrieve container " + currContainer.User + "/" + currContainer.Name);
                        continue;
                    }

                    #endregion

                    #region Process-Container

                    while (running)
                    {
                        #region Retrieve-Object-List

                        md.Params.Index = currIndex;

                        if (!_OutboundReplicationMgr.ContainerEnumerate(md, SourceNode, out currMetadata))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " unable to enumerate container " + currContainer.User + "/" + currContainer.Name + " on node ID " + SourceNode.NodeId);
                            running = false;
                            break;
                        }

                        if (currMetadata.Objects == null || currMetadata.Objects.Count < 1)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "ResyncWorker BackgroundTask " + WorkerGuid + " reached end of container " + currContainer.User + "/" + currContainer.Name + " on node ID " + SourceNode.NodeId);
                            running = false;
                            return;
                        }

                        currIndex += currMetadata.Objects.Count;

                        #endregion

                        #region Process-Each-Object

                        foreach (ObjectMetadata currObject in currMetadata.Objects)
                        {
                            #region Enumerate

                            _Logging.Log(LoggingModule.Severity.Debug, "ResyncWorker BackgroundTask " + WorkerGuid + " processing object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + " on node ID " + SourceNode.NodeId);

                            #endregion

                            #region Variables

                            ObjectMetadata currObjectMetadata = null;
                            byte[] objectBytes = null;

                            #endregion

                            #region Delete-if-Mismatch

                            container.ReadObjectMetadata(currObject.Key, out currObjectMetadata);

                            if (container.Exists(currObject.Key))
                            {
                                if (!container.ReadObjectMetadata(currObject.Key, out currObjectMetadata))
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " unable to read metadata for existing object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key);
                                    continue;
                                }

                                if (!currObjectMetadata.Md5.Equals(currObject.Md5))
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " deleting existing object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + ", MD5 mismatch");
                                    container.RemoveObject(currObject.Key, out error);
                                }
                                else
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " existing object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + " is up to date, skipping");
                                    continue;
                                }
                            } 

                            #endregion

                            #region Retrieve-and-Add

                            if (currObject.ContentLength > _Settings.Server.MaxTransferSize)
                            { 
                                #region Retrieve-Partial

                                bool cleanupRequired = false;

                                long currPosition = 0;
                                long bytesRemaining = Convert.ToInt64(currObject.ContentLength);
                                
                                md = BuildMetadata(null, null, currContainer.User, currContainer.Name, currObject.Key, "get");
                                md.Params.Index = 0;
                                md.Params.Count = _Settings.Server.MaxTransferSize;
                                long currIteration = 1;
                                long totalIterations = Convert.ToInt64(currObject.ContentLength) / _Settings.Server.MaxTransferSize;
                                if (Convert.ToInt64(currObject.ContentLength) % _Settings.Server.MaxTransferSize != 0) totalIterations++;
                                 
                                while (bytesRemaining > 0)
                                { 
                                    if (!_OutboundReplicationMgr.ObjectRead(md, SourceNode, out objectBytes))
                                    {
                                        _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " unable to retrieve " + 
                                            currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + 
                                            " [iteration " + currIteration + "/" + totalIterations + "]");
                                        continue;
                                    }

                                    if (currIteration == 1)
                                    { 
                                        #region Write-With-Metadata

                                        if (!container.WriteObject(currObject, objectBytes, out error))
                                        {
                                            _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " unable to write " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + ": " + error.ToString());
                                            cleanupRequired = true;
                                            break;
                                        }

                                        #endregion

                                        #region Update-Metadata-for-Next-Read

                                        currIteration++;
                                        currPosition += _Settings.Server.MaxTransferSize;
                                        bytesRemaining -= _Settings.Server.MaxTransferSize;

                                        if (bytesRemaining > 0)
                                        {
                                            if (bytesRemaining > _Settings.Server.MaxTransferSize)
                                            {
                                                #region More-Max-Size-Reads

                                                md.Params.Index += _Settings.Server.MaxTransferSize;
                                                md.Params.Count = _Settings.Server.MaxTransferSize;

                                                #endregion
                                            }
                                            else
                                            {
                                                #region Non-Max-Size-Read

                                                md.Params.Index += _Settings.Server.MaxTransferSize;
                                                md.Params.Count = bytesRemaining;

                                                #endregion
                                            }
                                        }
                                        else
                                        {
                                            break;
                                        }
                                        
                                        #endregion
                                    }
                                    else
                                    { 
                                        #region Write-Range

                                        if (!container.WriteRangeObject(currObject.Key, currPosition, objectBytes, out error))
                                        {
                                            _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " unable to write " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + ": " + error.ToString());
                                            cleanupRequired = true;
                                            break;
                                        }

                                        #endregion

                                        #region Update-Metadata-for-Next-Read

                                        currIteration++;
                                        currPosition += objectBytes.Length;
                                        bytesRemaining -= objectBytes.Length;

                                        if (bytesRemaining > 0)
                                        {
                                            if (bytesRemaining > _Settings.Server.MaxTransferSize)
                                            {
                                                #region More-Max-Size-Reads

                                                md.Params.Index += _Settings.Server.MaxTransferSize;
                                                md.Params.Count = _Settings.Server.MaxTransferSize;

                                                #endregion
                                            }
                                            else
                                            {
                                                #region Non-Max-Size-Read

                                                md.Params.Index += _Settings.Server.MaxTransferSize;
                                                md.Params.Count = bytesRemaining;

                                                #endregion
                                            }
                                        }
                                        else
                                        {
                                            break;
                                        }

                                        #endregion
                                    }
                                }

                                if (cleanupRequired)
                                {
                                    container.RemoveObject(currObject.Key, out error);
                                    _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " unable to retrieve " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + ", removing");
                                }

                                #endregion
                            }
                            else
                            {
                                #region Retrieve-Full

                                md = BuildMetadata(null, null, currContainer.User, currContainer.Name, currObject.Key, "get");
                                if (!_OutboundReplicationMgr.ObjectRead(md, SourceNode, out objectBytes))
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " unable to retrieve " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key);
                                    continue;
                                }

                                if (!container.WriteObject(currObject, objectBytes, out error))
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ResyncWorker BackgroundTask " + WorkerGuid + " unable to write " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key);
                                    continue;
                                }
                                
                                #endregion
                            }

                            #endregion 
                        }

                        running = false;

                        #endregion
                    }

                    #endregion
                }

                #endregion
            }
            catch (OperationCanceledException)
            {
                _Logging.Log(LoggingModule.Severity.Info, "ResyncWorker BackgroundTask " + WorkerGuid + " terminated");
            }
            catch (Exception e)
            {
                _Logging.LogException("ResyncWorker", "BackgroundTask", e);
            }
            finally
            {
                EndTime = DateTime.Now.ToUniversalTime();
                TotalMilliseconds = Common.TotalMsFrom(Convert.ToDateTime(StartTime));

                _Logging.Log(LoggingModule.Severity.Info, "ResyncWorker BackgroundTask " + WorkerGuid + " completed");

                _Running = false;
                _TaskComplete(WorkerGuid);
            }
        }

        private RequestMetadata BuildMetadata(int? indexStart, int? maxResults, string userGuid, string container, string objectKey, string method)
        {
            Node localNode = _Topology.LocalNode; 
            RequestMetadata ret = new RequestMetadata();
            ret.Params = new RequestMetadata.Parameters();
            ret.Params.UserGuid = userGuid;
            ret.Params.Container = container;
            ret.Params.ObjectKey = objectKey;
            ret.Params.Index = indexStart;
            ret.Params.Count = maxResults;
            ret.Params.OrderBy = "ORDER BY LastUpdateUtc ASC ";

            ret.Http = new HttpRequest();
            ret.Http.SourceIp = "127.0.0.1";
            ret.Http.SourcePort = 0;
            ret.Http.Method = method;

            ret.Http.RawUrlWithoutQuery = "/";
            if (!String.IsNullOrEmpty(userGuid))
            {
                ret.Http.RawUrlWithoutQuery += userGuid + "/";
                if (!String.IsNullOrEmpty(container))
                {
                    ret.Http.RawUrlWithoutQuery += container + "/";
                    if (!String.IsNullOrEmpty(objectKey))
                    {
                        ret.Http.RawUrlWithoutQuery += objectKey;
                    }
                }
            }

            ret.Http.TimestampUtc = DateTime.Now.ToUniversalTime();
            return ret;
        }

        #endregion
    }
}
