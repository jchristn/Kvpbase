using System;
using System.Collections.Generic; 
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;

using Kvpbase.Classes.BackgroundThreads;
using Kvpbase.Classes.Handlers;
using Kvpbase.Classes.Messaging;
using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase.Classes.Managers
{
    public class ResyncManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private TopologyManager _Topology;
        private OutboundHandler _Outbound;
        private ContainerManager _ContainerMgr;
        private ContainerHandler _Containers;
        private ObjectHandler _Objects;

        private readonly object _Lock;
        private Dictionary<string, ResyncWorker> _ResyncTasks;

        #endregion

        #region Constructors-and-Factories

        public ResyncManager(
            Settings settings,
            LoggingModule logging,
            TopologyManager topology,   
            OutboundHandler replication,
            ContainerManager containerMgr,
            ContainerHandler containers,
            ObjectHandler objects)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (topology == null) throw new ArgumentNullException(nameof(topology));
            if (replication == null) throw new ArgumentNullException(nameof(replication));
            if (containerMgr == null) throw new ArgumentNullException(nameof(containerMgr));
            if (containers == null) throw new ArgumentNullException(nameof(containers));
            if (objects == null) throw new ArgumentNullException(nameof(objects));

            _Settings = settings;
            _Logging = logging;
            _Topology = topology; 
            _Outbound = replication;
            _ContainerMgr = containerMgr;
            _Containers = containers;
            _Objects = objects;

            _Lock = new object();
            _ResyncTasks = new Dictionary<string, ResyncWorker>();
        }

        #endregion

        #region Public-Methods

        public bool Exists(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            lock (_Lock)
            {
                return _ResyncTasks.ContainsKey(guid);
            }
        }

        public bool Exists(Node node, string userName, string container, DateTime? startTime)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            lock (_Lock)
            {
                return _ResyncTasks.Any(t => 
                    t.Value.SourceNode == node 
                    && t.Value.UserName.Equals(userName) 
                    && t.Value.ContainerName.Equals(container) 
                    && t.Value.StartTime.Equals(startTime));
            }
        }

        public bool Add(Node node, DateTime? startTime, out List<string> taskGuids)
        {
            taskGuids = new List<string>();
            if (node == null) throw new ArgumentNullException(nameof(node));

            // get list of containers
            RequestMetadata md = RequestMetadata.Default(); 
            List<ContainerSettings> containers = null;
            if (!_Outbound.ContainerList(md, node, out containers))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Add unable to retrieve container list from node " + node.ToString());
                return false;
            }

            if (containers != null && containers.Count > 0)
            {
                foreach (ContainerSettings curr in containers)
                {
                    string taskGuid = null;
                    if (!Add(node, curr.User, curr.Name, startTime, out taskGuid))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Add unable to add task for container " + curr.User + "/" + curr.Name);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "ResyncManager Add added task for container " + curr.User + "/" + curr.Name);
                        taskGuids.Add(taskGuid);
                    }
                }
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Info, "ResyncManager Add no containers found on node " + node.ToString()); 
            }

            return true;
        }

        public bool Add(Node node, string userName, DateTime? startTime, out List<string> taskGuids)
        {
            taskGuids = new List<string>();
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (String.IsNullOrEmpty(userName)) throw new ArgumentNullException(nameof(userName));

            // get list of containers
            RequestMetadata md = RequestMetadata.Default();
            List<ContainerSettings> containers = null;
            if (!_Outbound.ContainerList(md, node, out containers))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Add unable to retrieve container list from node " + node.ToString());
                return false;
            }

            if (containers != null && containers.Count > 0)
            {
                foreach (ContainerSettings curr in containers)
                {
                    if (String.IsNullOrEmpty(curr.User)) continue;

                    if (curr.User.ToLower().Trim().Equals(userName.ToLower().Trim()))
                    {
                        string taskGuid = null;
                        if (!Add(node, curr.User, curr.Name, startTime, out taskGuid))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Add unable to add task for container " + curr.User + "/" + curr.Name);
                        }
                        else
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "ResyncManager Add added task for container " + curr.User + "/" + curr.Name);
                            taskGuids.Add(taskGuid);
                        }
                    }
                }
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Info, "ResyncManager Add no containers found on node " + node.ToString()); 
            }

            return true;
        }

        public bool Add(Node node, string userName, string container, DateTime? startTime, out string taskGuid)
        {
            taskGuid = null;
            if (node == null) throw new ArgumentNullException(nameof(node));

            if (Exists(node, userName, container, startTime))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Add failed, task already exists");
                return false;
            }

            taskGuid = Guid.NewGuid().ToString();
            ResyncWorker worker = new ResyncWorker(
                _Settings, 
                _Logging, 
                _Topology, 
                _Outbound, 
                _ContainerMgr, 
                _Containers, 
                _Objects, 
                node, 
                userName, 
                container, 
                startTime,
                taskGuid,
                Stop);
             
            lock (_Lock)
            {
                _ResyncTasks.Add(taskGuid, worker);
            }

            _Logging.Log(LoggingModule.Severity.Debug, "ResyncManager Add added task " + taskGuid);
            return true;
        }

        public bool Start(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            if (!Exists(guid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Start unable to find worker GUID " + guid);
                return false;
            }

            ResyncWorker worker = Get(guid);
            worker.Start();

            return true;
        }

        public bool StartAll(out List<string> started)
        {
            started = new List<string>();
            Dictionary<string, ResyncWorker> tasks = new Dictionary<string, ResyncWorker>();

            lock (_Lock)
            {
                tasks = new Dictionary<string, ResyncWorker>(_ResyncTasks);
            }

            if (_ResyncTasks == null || _ResyncTasks.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Info, "ResyncManager StartAll no tasks defined");
                return true;
            }

            foreach (KeyValuePair<string, ResyncWorker> kvp in tasks)
            {
                if (!kvp.Value.IsRunning)
                {
                    _Logging.Log(LoggingModule.Severity.Info, "ResyncManager StartAll starting task " + kvp.Key);
                    kvp.Value.Start();
                    started.Add(kvp.Key);
                }
            }

            return true;
        }

        public bool Stop(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            if (!Exists(guid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Stop unable to find worker GUID " + guid);
                return false;
            }

            ResyncWorker worker = Get(guid);
            worker.Stop();

            lock (_Lock)
            {
                if (_ResyncTasks.ContainsKey(guid))
                {
                    _ResyncTasks.Remove(guid);
                }
            }

            return true;
        }

        public ResyncWorker.Statistics Stats(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid)); 
            ResyncWorker curr = Get(guid);
            if (curr != null) return curr.Stats;
            return null;
        }

        public ResyncWorker Get(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
             
            lock (_Lock)
            {
                if (_ResyncTasks.ContainsKey(guid))
                {
                    return _ResyncTasks[guid];
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Get unable to find worker GUID " + guid);
                    return null;
                }
            }
        }

        public List<ResyncWorker> GetTasks()
        {
            lock (_Lock)
            {
                return _ResyncTasks.Values.ToList();
            }
        }
        
        public void Remove(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            if (!Exists(guid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Remove unable to find worker GUID " + guid);
                return;
            }

            ResyncWorker worker = Get(guid);
            worker.Stop();

            lock (_Lock)
            {
                if (_ResyncTasks.ContainsKey(guid)) _ResyncTasks.Remove(guid);
            } 
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
