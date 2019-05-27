using System;
using System.Collections.Generic; 
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;

using Kvpbase.Classes.BackgroundThreads;
using Kvpbase.Classes.Handlers;
using Kvpbase.Classes.Messaging;
using Kvpbase.Container;

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
        private OutboundMessageHandler _OutboundReplicationMgr;
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
            OutboundMessageHandler replication,
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
            _OutboundReplicationMgr = replication;
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

        public bool Add(Node node, string userName, string container, DateTime? startTime, out string guid)
        {
            guid = null;
            if (node == null) throw new ArgumentNullException(nameof(node));

            if (Exists(node, userName, container, startTime))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Add failed, task already exists");
                return false;
            }

            guid = Guid.NewGuid().ToString();
            ResyncWorker worker = new ResyncWorker(
                _Settings, 
                _Logging, 
                _Topology, 
                _OutboundReplicationMgr, 
                _ContainerMgr, 
                _Containers, 
                _Objects, 
                node, 
                userName, 
                container, 
                startTime,
                guid,
                Stop);
             
            lock (_Lock)
            {
                _ResyncTasks.Add(guid, worker);
            }

            _Logging.Log(LoggingModule.Severity.Debug, "ResyncManager Add added task " + guid);
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

        public bool Stats()
        {
            return false; 
        }

        public ResyncWorker Get(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            if (!Exists(guid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ResyncManager Get unable to find worker GUID " + guid);
                return null;
            }

            lock (_Lock)
            {
                return _ResyncTasks[guid];
            }
        }

        public List<ResyncWorker> GetTasks()
        {
            lock (_Lock)
            {
                return _ResyncTasks.Values.ToList();
            }
        }
         
        #endregion

        #region Private-Methods

        #endregion
    }
}
