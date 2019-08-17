using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Classes.Handlers;
using Kvpbase.Classes.Managers;
using Kvpbase.Classes.Messaging;
using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase.Classes.BackgroundThreads
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

        public bool IsRunning
        {
            get
            {
                return _Running;
            }
        }

        public Statistics Stats
        {
            get
            {
                return _Stats;
            }
        }

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private TopologyManager _Topology;
        private OutboundHandler _OutboundMessageHandler;
        private ContainerManager _ContainerMgr;
        private ContainerHandler _Containers;
        private ObjectHandler _Objects;

        private Statistics _Stats = new Statistics();
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
            OutboundHandler outbound,
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
            if (outbound == null) throw new ArgumentNullException(nameof(outbound));
            if (containerMgr == null) throw new ArgumentNullException(nameof(containerMgr));
            if (containers == null) throw new ArgumentNullException(nameof(containers));
            if (objects == null) throw new ArgumentNullException(nameof(objects));
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (taskComplete == null) throw new ArgumentNullException(nameof(taskComplete));

            _Settings = settings;
            _Logging = logging;
            _Topology = topology;
            _OutboundMessageHandler = outbound;
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
                _Logging.Warn("ResyncWorker unable to verify node ID " + SourceNode.NodeId + " in topology");
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
                _Logging.Warn("ResyncWorker Start already running for node " + SourceNode.ToString());
                return false;
            }

            _Running = true;
            StartTime = DateTime.Now.ToUniversalTime();

            Task.Run(() => BackgroundTask(), _Token);
            return true;
        }

        public void Stop()
        {
            _Logging.Warn("ResyncWorker " + WorkerGuid + " cancellation requested");
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

                RequestMetadata md = BuildMetadata(null, null, UserName, ContainerName, null, HttpMethod.GET);
                if (!_OutboundMessageHandler.ContainerList(md, SourceNode, out containers))
                {
                    _Logging.Warn("ResyncWorker BackgroundTask unable to retrieve container list from " + SourceNode.ToString());
                    return;
                }
                 
                if (containers == null || containers.Count < 1)
                {
                    _Logging.Warn("ResyncWorker BackgroundTask no containers found on node " + SourceNode.ToString());
                    return;
                }

                if (!String.IsNullOrEmpty(UserName) && !String.IsNullOrEmpty(ContainerName))
                {
                    // Filter list
                    foreach (ContainerSettings currContainer in containers)
                    {
                        if (currContainer.User.ToLower().Equals(UserName.ToLower())
                            && currContainer.Name.ToLower().Equals(ContainerName.ToLower()))
                        {
                            filtered.Add(currContainer); 
                        } 
                    }

                    if (filtered == null || filtered.Count < 1)
                    {
                        _Logging.Warn("ResyncWorker BackgroundTask no matching containers found on node " + SourceNode.ToString());
                        return;
                    }

                    Containers = new List<ContainerSettings>(filtered);
                }
                else
                {
                    Containers = new List<ContainerSettings>(containers);
                }

                Stats.ContainerCount = Containers.Count;

                #endregion

                #region Process-Each-Container

                foreach (ContainerSettings currContainer in Containers)
                {
                    #region Pre-Enumerate

                    _Logging.Info("ResyncWorker BackgroundTask " + WorkerGuid + " starting container " + currContainer.User + "/" + currContainer.Name + " on node ID " + SourceNode.NodeId);

                    #endregion

                    #region Variables

                    int? currIndex = 0;
                    md = BuildMetadata(currIndex, null, currContainer.User, currContainer.Name, null, HttpMethod.GET);
                    ContainerMetadata currMetadata = new ContainerMetadata();
                    bool running = true;
                    Container container = null;
                    ErrorCode error;

                    #endregion

                    #region Retrieve-or-Create-Container

                    if (!_ContainerMgr.Exists(currContainer.User, currContainer.Name))
                    {
                        _Logging.Info("ResyncWorker BackgroundTask " + WorkerGuid + " creating container " + currContainer.User + "/" + currContainer.Name);
                        _ContainerMgr.Add(currContainer);
                    }
                    
                    if (!_ContainerMgr.GetContainer(currContainer.User, currContainer.Name, out container))
                    {
                        _Logging.Warn("ResyncWorker BackgroundTask " + WorkerGuid + " unable to retrieve container " + currContainer.User + "/" + currContainer.Name);
                        Stats.Errors.Add("Unable to retrieve container after adding: " + currContainer.User + "/" + currContainer.Name);
                        Stats.ContainersProcessed++;
                        continue;
                    }

                    #endregion

                    #region Process-Container

                    while (running)
                    {
                        #region Retrieve-Object-List

                        md.Params.Index = currIndex;

                        if (!_OutboundMessageHandler.ContainerEnumerate(md, SourceNode, out currMetadata))
                        {
                            _Logging.Warn("ResyncWorker BackgroundTask " + WorkerGuid + " unable to enumerate container " + currContainer.User + "/" + currContainer.Name + " on node ID " + SourceNode.NodeId);
                            Stats.Errors.Add("Unable to enumerate container " + currContainer.User + "/" + currContainer.Name + " on node ID " + SourceNode.NodeId);
                            Stats.ContainersProcessed++;
                            running = false;
                            break;
                        }

                        if (currMetadata.Objects == null || currMetadata.Objects.Count < 1)
                        {
                            _Logging.Debug("ResyncWorker BackgroundTask " + WorkerGuid + " reached end of container " + currContainer.User + "/" + currContainer.Name + " on node ID " + SourceNode.NodeId);
                            running = false;
                            break;
                        }

                        currIndex += currMetadata.Objects.Count;
                        Stats.ObjectCount += currMetadata.Objects.Count;
                        Stats.BytesCount += currMetadata.Objects.Sum(x => Convert.ToInt64(x.ContentLength));

                        #endregion

                        #region Process-Each-Object

                        foreach (ObjectMetadata currObject in currMetadata.Objects)
                        {
                            #region Enumerate

                            _Logging.Debug("ResyncWorker BackgroundTask " + WorkerGuid + " processing object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + " on node ID " + SourceNode.NodeId);

                            #endregion

                            #region Variables

                            ObjectMetadata currObjectMetadata = null;
                            long contentLength = 0;
                            Stream stream = null;

                            #endregion

                            #region Delete-if-Mismatch

                            container.ReadObjectMetadata(currObject.Key, out currObjectMetadata);

                            if (container.Exists(currObject.Key))
                            {
                                if (!container.ReadObjectMetadata(currObject.Key, out currObjectMetadata))
                                {
                                    _Logging.Warn("ResyncWorker BackgroundTask " + WorkerGuid + " unable to read metadata for existing object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key);
                                    Stats.Errors.Add("Unable to read metadata for object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key);
                                    Stats.ObjectsProcessed++;
                                    continue;
                                }

                                if (!currObjectMetadata.Md5.Equals(currObject.Md5))
                                {
                                    _Logging.Warn("ResyncWorker BackgroundTask " + WorkerGuid + " deleting existing object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + ", MD5 mismatch");
                                    Stats.Errors.Add("Removed object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + " due to MD5 mismatch");
                                    Stats.ObjectsProcessed++;
                                    container.RemoveObject(currObject.Key, out error);
                                }
                                else
                                {
                                    _Logging.Warn("ResyncWorker BackgroundTask " + WorkerGuid + " existing object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + " is up to date, skipping");
                                    Stats.ObjectsProcessed++;
                                    Stats.BytesProcessed += Convert.ToInt64(currObject.ContentLength);
                                    continue;
                                }
                            } 

                            #endregion

                            #region Retrieve-and-Add
                             
                            md = BuildMetadata(null, null, currContainer.User, currContainer.Name, currObject.Key, HttpMethod.GET);
                            if (!_OutboundMessageHandler.ObjectRead(md, SourceNode, out contentLength, out stream))
                            {
                                _Logging.Warn("ResyncWorker BackgroundTask " + WorkerGuid + " unable to retrieve " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key);
                                Stats.Errors.Add("Unable to retrieve " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + " from node ID " + SourceNode.NodeId);
                                Stats.ObjectsProcessed++;
                                Stats.BytesProcessed += Convert.ToInt64(currObject.ContentLength);
                                continue;
                            }

                            if (!container.WriteObject(currObject, stream, out error))
                            {
                                _Logging.Warn("ResyncWorker BackgroundTask " + WorkerGuid + " unable to write " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + ": " + error.ToString());
                                Stats.Errors.Add("Unable to write object " + currContainer.User + "/" + currContainer.Name + "/" + currObject.Key + ": " + error.ToString());
                                Stats.ObjectsProcessed++;
                                Stats.BytesProcessed += Convert.ToInt64(currObject.ContentLength);
                                continue;
                            }

                            Stats.ObjectsProcessed++;
                            Stats.BytesProcessed += Convert.ToInt64(currObject.ContentLength);

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
                _Logging.Info("ResyncWorker BackgroundTask " + WorkerGuid + " terminated");
            }
            catch (Exception e)
            {
                _Logging.Exception("ResyncWorker", "BackgroundTask", e);
            }
            finally
            {
                EndTime = DateTime.Now.ToUniversalTime();
                TotalMilliseconds = Common.TotalMsFrom(Convert.ToDateTime(StartTime));

                _Logging.Info("ResyncWorker BackgroundTask " + WorkerGuid + " completed");

                _Running = false;
                _TaskComplete(WorkerGuid);
            }
        }

        private RequestMetadata BuildMetadata(int? indexStart, int? maxResults, string userGuid, string container, string objectKey, HttpMethod method)
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

        #region Embedded-Classes

        public class Statistics
        {
            public long ContainerCount = 0;
            public long ContainersProcessed = 0;
            public long ObjectCount = 0;
            public long ObjectsProcessed = 0;
            public long BytesCount = 0;
            public long BytesProcessed = 0;
            public List<string> Errors = new List<string>();
        }

        #endregion
    }
}
