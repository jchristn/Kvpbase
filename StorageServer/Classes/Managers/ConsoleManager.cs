using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using RestWrapper;
using WatsonWebserver;

using Kvpbase.Classes.BackgroundThreads;
using Kvpbase.Classes.Handlers;
using Kvpbase.Classes.Messaging;

using Kvpbase.Container;

namespace Kvpbase.Classes.Managers
{
    public class ConsoleManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private bool _Enabled { get; set; }
        private Settings _Settings { get; set; }
        private TopologyManager _Topology { get; set; } 
        private UserManager _UserMgr { get; set; }
        private UrlLockManager _UrlLockMgr { get; set; }
        private EncryptionManager _Encryption { get; set; }
        private LoggingModule _Logging { get; set; } 
        private OutboundMessageHandler _OutboundMessageHandler { get; set; }
        private ContainerManager _ContainerMgr { get; set; }
        private ContainerHandler _Containers { get; set; }
        private ObjectHandler _Objects { get; set; }
        private ResyncManager _ResyncMgr { get; set; }
        private Func<bool> _ExitDelegate;

        private static string _TimestampFormat = "yyyy-MM-ddTHH:mm:ss.ffffffZ";

        #endregion

        #region Constructors-and-Factories

        public ConsoleManager(
            Settings settings,
            LoggingModule logging,
            TopologyManager topology,  
            UserManager users, 
            UrlLockManager locks,
            EncryptionManager encryption, 
            OutboundMessageHandler replication,
            ContainerManager containerMgr,
            ContainerHandler containers,
            ObjectHandler objects,
            ResyncManager resync,
            Func<bool> exitApplication)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings)); 
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (topology == null) throw new ArgumentNullException(nameof(topology));
            if (users == null) throw new ArgumentNullException(nameof(users));
            if (locks == null) throw new ArgumentNullException(nameof(locks)); 
            if (encryption == null) throw new ArgumentNullException(nameof(encryption));
            if (replication == null) throw new ArgumentNullException(nameof(replication));
            if (containerMgr == null) throw new ArgumentNullException(nameof(containerMgr));
            if (containers == null) throw new ArgumentNullException(nameof(containers));
            if (objects == null) throw new ArgumentNullException(nameof(objects));
            if (resync == null) throw new ArgumentNullException(nameof(resync));
            if (exitApplication == null) throw new ArgumentNullException(nameof(exitApplication));

            _Enabled = true;

            _Settings = settings;
            _Logging = logging;
            _Topology = topology; 
            _UserMgr = users;
            _UrlLockMgr = locks;
            _Encryption = encryption;
            _OutboundMessageHandler = replication;
            _ContainerMgr = containerMgr;
            _Containers = containers;
            _Objects = objects;
            _ResyncMgr = resync;
            _ExitDelegate = exitApplication;

            Task.Run(() => ConsoleWorker());
        }

        #endregion

        #region Public-Methods

        public void Stop()
        {
            _Enabled = false;
            return;
        }

        #endregion

        #region Private-Methods

        private void ConsoleWorker()
        {
            string userInput = ""; 

            while (_Enabled)
            {
                Console.Write("Command (? for help) > ");
                userInput = Console.ReadLine();

                if (userInput == null) continue;
                switch (userInput.ToLower().Trim())
                {
                    case "?":
                        Menu();
                        break;

                    case "c":
                    case "cls":
                    case "clear":
                        Console.Clear();
                        break;

                    case "q":
                    case "quit":
                        _Enabled = false;
                        _ExitDelegate();
                        break;
                         
                    case "topology":
                        ListTopology();
                        break;

                    case "hello":
                        _Topology.SayHello();
                        break;

                    case "send async":
                        SendAsyncConsoleMessage();
                        break;

                    case "send sync":
                        SendSyncConsoleMessage();
                        break;

                    case "active":
                        ListActiveUrls();
                        break;
                         
                    case "version":
                        System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
                        FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
                        string version = fvi.FileVersion;
                        Console.WriteLine(version);
                        break;

                    case "debug_on":
                        _Settings.Syslog.MinimumLevel = 0;
                        break;

                    case "debug_off":
                        _Settings.Syslog.MinimumLevel = 1;
                        break;

                    case "containers":
                        ListContainers();
                        break;

                    case "container exists":
                        ContainerExists();
                        break;

                    case "container delete":
                        ContainerDelete();
                        break;

                    case "container enumerate":
                        ContainerEnumerate();
                        break;

                    case "object read":
                        ObjectRead();
                        break;

                    case "object exists":
                        ObjectExists();
                        break;

                    case "object delete":
                        ObjectDelete();
                        break;

                    case "object metadata":
                        ObjectMetadata();
                        break;

                    case "latest":
                        GetLatestTimestamp();
                        break;

                    case "sync add":
                        SyncAdd();
                        break;

                    case "sync tasks":
                        SyncTasks();
                        break;

                    case "sync start":
                        SyncStart();
                        break;

                    default:
                        Console.WriteLine("Unknown command.  '?' for help.");
                        break;
                }
            }
        }

        private void Menu()
        {
            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("  ?                         help / this menu");
            Console.WriteLine("  cls / c                   clear the console");
            Console.WriteLine("  quit / q                  exit the application");  
            Console.WriteLine("  topology                  list nodes in the topology");
            Console.WriteLine("  hello                     say hello to other nodes in the topology");
            Console.WriteLine("  send async                send async message to another node");
            Console.WriteLine("  send sync                 send sync message to another node which will echo back");
            Console.WriteLine("  active                    list URLs that are being read or written");
            Console.WriteLine("  containers                list available containers");
            Console.WriteLine("  container exists          query if a container exists");
            Console.WriteLine("  container delete          delete a container");
            Console.WriteLine("  container enumerate       enumerate contents of a container");
            Console.WriteLine("  object read               read contents of an object");
            Console.WriteLine("  object exists             query if an object exists");
            Console.WriteLine("  object delete             delete an object");
            Console.WriteLine("  object metadata           retrieve metadata of an object");
            Console.WriteLine("  latest                    get timestamp of latest entry in a container");
            Console.WriteLine("  sync add                  add synchronization task");
            Console.WriteLine("  sync tasks                list configured synchronization tasks");
            Console.WriteLine("  sync start                start a synchronization task");
            Console.WriteLine("  version                   show the product version");
            Console.WriteLine("");
            return;
        }
          
        private void ListTopology()
        {
            if (_Topology == null)
            {
                Console.WriteLine("Topology contains no nodes");
            }
            else
            {
                Console.WriteLine("");
                Console.WriteLine("Topology");
                Console.WriteLine("  * represents local node (ID " + _Topology.LocalNode.NodeId + ")");
                Console.WriteLine("  ! represents failed node"); 
                Console.WriteLine("");
                Console.WriteLine("All nodes:");
                List<Node> nodes = _Topology.GetNodes();

                if (nodes != null && nodes.Count > 0)
                {
                    foreach (Node curr in _Topology.GetNodes())
                    { 
                        if (_Topology.IsNodeHealthy(curr))
                        {
                            if (curr.NodeId == _Topology.LocalNode.NodeId) Console.WriteLine("  * " + curr.ToString());
                            else Console.WriteLine("  " + curr.ToString());
                        }
                        else
                        {
                            Console.WriteLine("  ! " + curr.ToString());
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Topology contains no nodes");
                }

                Console.WriteLine("");
                Console.WriteLine("Replicas:");
                List<Node> replicas = _Topology.GetReplicas();
                if (replicas != null && replicas.Count > 0)
                {
                    foreach (Node curr in _Topology.GetReplicas())
                    {
                        if (_Topology.IsNodeHealthy(curr))
                        {
                            if (curr.NodeId == _Topology.LocalNode.NodeId)
                                Console.WriteLine("  * " + curr.ToString());
                            else
                                Console.WriteLine("  " + curr.ToString());
                        }
                        else
                        {
                            Console.WriteLine("  ! " + curr.ToString());
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Topology contains no replicas");
                }

                Console.WriteLine("");
                Console.WriteLine("Healthy: " + _Topology.IsNetworkHealthy());
            }

            Console.WriteLine("");
        }

        private void SendAsyncConsoleMessage()
        {
            ListTopology();
            int nodeId = Common.InputInteger("Node ID:", 0, true, false);
            string msg = Common.InputString("Message:", "Hello, world!", false);
            _Topology.SendAsyncMessage(MessageType.Console, nodeId, Encoding.UTF8.GetBytes(msg));
        }

        private void SendSyncConsoleMessage()
        {
            ListTopology();
            int nodeId = Common.InputInteger("Node ID:", 0, true, false);
            string msg = Common.InputString("Message:", "Hello, world!", false);
            Message resp = _Topology.SendSyncMessage(MessageType.Echo, nodeId, Encoding.UTF8.GetBytes(msg));
            if (resp == null)
            {
                Console.WriteLine("Failed");
            }
            else
            {
                Console.WriteLine("Success");
                Console.WriteLine(Encoding.UTF8.GetString(resp.Data));
            }
        }

        private void ListActiveUrls()
        {
            Dictionary<string, LockedResource> writeLocked = _UrlLockMgr.GetWriteLockedUrls();
            List<string> readLocked = _UrlLockMgr.GetReadLockedUrls();

            Console.WriteLine("Write locks:");
            if (writeLocked == null || writeLocked.Count < 1)
            {
                Console.WriteLine("  None");
            }
            else
            {
                foreach (KeyValuePair<string, LockedResource> currLock in writeLocked)
                {
                    Console.WriteLine("  " + currLock.Value.ToString());
                }
            }

            Console.WriteLine("Read locks:");
            if (readLocked == null || readLocked.Count < 1)
            {
                Console.WriteLine("  None");
            }
            else
            {
                foreach (string currLock in readLocked)
                {
                    Console.WriteLine("  " + currLock);
                }
            } 
        }

        private void ListContainers()
        {
            List<ContainerSettings> containers = new List<ContainerSettings>();

            int nodeId = Common.InputInteger("Node ID [0 for local]:", 0, true, true);
            if (nodeId == 0)
            {
                if (!_ContainerMgr.GetContainers(out containers))
                {
                    Console.WriteLine("Failed");
                }
                else
                {
                    Console.WriteLine("Containers:");
                    if (containers == null || containers.Count < 1)
                    {
                        Console.WriteLine("None");
                    }
                    else
                    {
                        foreach (ContainerSettings currSettings in containers)
                        {
                            Console.WriteLine("  " + currSettings.User + "/" + currSettings.Name);
                        }
                    }
                }
            }
            else
            {
                Node node = _Topology.GetNodeById(nodeId);
                if (node == null)
                {
                    Console.WriteLine("Unknown node");
                    return;
                }

                RequestMetadata md = BuildMetadata(null, null, null, WatsonWebserver.HttpMethod.GET);
                if (!_OutboundMessageHandler.ContainerList(md, node, out containers))
                {
                    Console.WriteLine("Request to node ID " + nodeId + " failed");
                }
                else
                {
                    Console.WriteLine(Common.SerializeJson(containers, true));
                }
            }
        }

        private void ContainerExists()
        {
            int nodeId = Common.InputInteger("Node ID [0 for local]:", 0, true, true);
            Node node = null;

            string userGuid = Common.InputString("User GUID:", "default", false);
            string containerName = Common.InputString("Container name:", "default", false);
            RequestMetadata md = BuildMetadata(userGuid, containerName, null, WatsonWebserver.HttpMethod.GET);

            if (nodeId > 0)
            {
                node = _Topology.GetNodeById(nodeId);
                if (node == null)
                {
                    Console.WriteLine("Unknown node");
                    return;
                }
            }

            if (nodeId == 0)
            {
                Console.WriteLine(_Containers.Exists(md, userGuid, containerName));
            }
            else
            {
                Console.WriteLine(_OutboundMessageHandler.ContainerExists(md, node));
            }
        }

        private void ContainerDelete()
        {
            int nodeId = Common.InputInteger("Node ID [0 for local]:", 0, true, true);
            Node node = null;

            string userGuid = Common.InputString("User GUID:", "default", false);
            string containerName = Common.InputString("Container name:", "default", false);
            RequestMetadata md = BuildMetadata(userGuid, containerName, null, WatsonWebserver.HttpMethod.GET);

            if (nodeId > 0)
            {
                node = _Topology.GetNodeById(nodeId);
                if (node == null)
                {
                    Console.WriteLine("Unknown node");
                    return;
                }
            }

            ContainerSettings settings = null;
            if (!_ContainerMgr.GetContainerSettings(userGuid, containerName, out settings))
            {
                Console.WriteLine("Unknown container");
                return;
            }

            if (nodeId == 0)
            {
                _Containers.Delete(userGuid, containerName);
            }
            else
            {
                Console.WriteLine(_OutboundMessageHandler.ContainerDelete(md, settings));
            }
        }

        private void ContainerEnumerate()
        {
            int nodeId = Common.InputInteger("Node ID [0 for local]:", 0, true, true);
            Node node = null;

            string userGuid = Common.InputString("User GUID:", "default", false);
            string containerName = Common.InputString("Container name:", "default", false);
            RequestMetadata md = BuildMetadata(userGuid, containerName, null, WatsonWebserver.HttpMethod.GET);

            if (nodeId > 0)
            {
                node = _Topology.GetNodeById(nodeId);
                if (node == null)
                {
                    Console.WriteLine("Unknown node");
                    return;
                }
            }

            Container.Container container = null;
            ContainerMetadata metadata = null;
            if (nodeId == 0)
            {
                if (!_ContainerMgr.GetContainer(userGuid, containerName, out container))
                {
                    Console.WriteLine("Unknown container");
                    return;
                }

                metadata = container.Enumerate(null, null, null, null);
                Console.WriteLine(Common.SerializeJson(metadata, true));
            }
            else
            {
                if (!_OutboundMessageHandler.ContainerEnumerate(md, node, out metadata))
                {
                    Console.WriteLine("Unable to query node ID " + nodeId);
                    return;
                }
                else
                {
                    Console.WriteLine(Common.SerializeJson(metadata, true));
                }
            }
        }

        private void ObjectRead()
        {
            int nodeId = Common.InputInteger("Node ID [0 for local]:", 0, true, true);
            Node node = null;

            string userGuid = Common.InputString("User GUID:", "default", false);
            string containerName = Common.InputString("Container name:", "default", false);
            string objectKey = Common.InputString("Object key:", "hello.txt", false);
            RequestMetadata md = BuildMetadata(userGuid, containerName, objectKey, WatsonWebserver.HttpMethod.GET);

            if (nodeId > 0)
            {
                node = _Topology.GetNodeById(nodeId);
                if (node == null)
                {
                    Console.WriteLine("Unknown node");
                    return;
                }
            }

            byte[] data = null;
            string contentType;
            ErrorCode error;

            if (nodeId == 0)
            {
                Container.Container container = null;
                if (!_ContainerMgr.GetContainer(userGuid, containerName, out container))
                {
                    Console.WriteLine("Unknown container");
                    return;
                }

                if (!container.ReadObject(objectKey, out contentType, out data, out error))
                {
                    Console.WriteLine("Failed (" + error.ToString() + ")");
                    return;
                }
                else
                {
                    Console.WriteLine(Encoding.UTF8.GetString(data));
                    Console.WriteLine(data.Length + " bytes, content type: " + contentType);
                    return;
                } 
            }
            else
            {
                if (!_OutboundMessageHandler.ObjectRead(md, node, out data))
                {
                    Console.WriteLine("Request failed to node ID " + node.NodeId);
                    return;
                }
                else
                {
                    Console.WriteLine(Encoding.UTF8.GetString(data));
                    Console.WriteLine(data.Length + " bytes");
                    return;
                }
            }
        }

        private void ObjectExists()
        {
            int nodeId = Common.InputInteger("Node ID [0 for local]:", 0, true, true);
            Node node = null;

            string userGuid = Common.InputString("User GUID:", "default", false);
            string containerName = Common.InputString("Container name:", "default", false);
            string objectKey = Common.InputString("Object key:", "hello.txt", false);
            RequestMetadata md = BuildMetadata(userGuid, containerName, objectKey, WatsonWebserver.HttpMethod.GET);

            if (nodeId > 0)
            {
                node = _Topology.GetNodeById(nodeId);
                if (node == null)
                {
                    Console.WriteLine("Unknown node");
                    return;
                }
            }

            if (nodeId == 0)
            {
                Container.Container container = null;
                if (!_ContainerMgr.GetContainer(userGuid, containerName, out container))
                {
                    Console.WriteLine("Unknown container");
                    return;
                }

                Console.WriteLine(_Objects.Exists(md, container, objectKey));
            }
            else
            {
                Console.WriteLine(_OutboundMessageHandler.ObjectExists(md, node));
            }
        }

        private void ObjectDelete()
        {
            int nodeId = Common.InputInteger("Node ID [0 for local]:", 0, true, true);
            Node node = null;

            string userGuid = Common.InputString("User GUID:", "default", false);
            string containerName = Common.InputString("Container name:", "default", false);
            string objectKey = Common.InputString("Object key:", "hello.txt", false);
            RequestMetadata md = BuildMetadata(userGuid, containerName, objectKey, WatsonWebserver.HttpMethod.DELETE);

            if (nodeId > 0)
            {
                node = _Topology.GetNodeById(nodeId);
                if (node == null)
                {
                    Console.WriteLine("Unknown node");
                    return;
                }
            }

            Container.Container container = null;
            ContainerSettings settings = null;
            ErrorCode error;

            if (nodeId == 0)
            {
                if (!_ContainerMgr.GetContainer(userGuid, containerName, out container))
                {
                    Console.WriteLine("Unknown container");
                    return;
                }

                Console.WriteLine(container.RemoveObject(objectKey, out error)); 
            }
            else
            {
                if (!_ContainerMgr.GetContainerSettings(userGuid, containerName, out settings))
                {
                    Console.WriteLine("Unknown container");
                    return;
                }

                bool disableReplication = Common.InputBoolean("Disable replication:", false);
                if (disableReplication) settings.Replication = ReplicationMode.None;

                Console.WriteLine(_OutboundMessageHandler.ObjectDelete(md, node));
            }
        }

        private void ObjectMetadata()
        {
            int nodeId = Common.InputInteger("Node ID [0 for local]:", 0, true, true);
            Node node = null; 
            ObjectMetadata metadata = null;

            string userGuid = Common.InputString("User GUID:", "default", false);
            string containerName = Common.InputString("Container name:", "default", false);
            string objectKey = Common.InputString("Object key:", "hello.txt", false);
            RequestMetadata md = BuildMetadata(userGuid, containerName, objectKey, WatsonWebserver.HttpMethod.GET);
            md.Params.Metadata = true;

            if (nodeId > 0)
            {
                node = _Topology.GetNodeById(nodeId);
                if (node == null)
                {
                    Console.WriteLine("Unknown node");
                    return;
                }
            }

            if (nodeId == 0)
            {
                Container.Container container = null;
                if (!_ContainerMgr.GetContainer(userGuid, containerName, out container))
                {
                    Console.WriteLine("Unknown container");
                    return;
                }

                if (!container.ReadObjectMetadata(objectKey, out metadata))
                {
                    Console.WriteLine("Not found");
                }
                else
                {
                    Console.WriteLine(Common.SerializeJson(metadata, true));
                }
            }
            else
            {
                if (!_OutboundMessageHandler.ObjectMetadata(md, node, out metadata))
                {
                    Console.WriteLine("Not found");
                }
                else
                {
                    Console.WriteLine(Common.SerializeJson(metadata, true));
                } 
            }
        }

        private void GetLatestTimestamp()
        { 
            string userGuid = Common.InputString("User GUID:", "default", false);
            string containerName = Common.InputString("Container name:", "default", false);

            Container.Container currContainer = null;
            if (!_ContainerMgr.GetContainer(userGuid, containerName, out currContainer))
            {
                Console.WriteLine("Unknown container");
            }
            else
            {
                DateTime? ts = currContainer.LatestEntry();
                if (ts == null)
                {
                    Console.WriteLine("None");
                }
                else
                {
                    Console.WriteLine(Convert.ToDateTime(ts).ToString(_TimestampFormat));
                }
            }
        }

        private void SyncAdd()
        {
            int nodeId = Common.InputInteger("Node ID:", 1, true, false);
            Node node = null;

            string userGuid = Common.InputString("User GUID:", "default", true);
            string containerName = Common.InputString("Container name:", "default", true);

            DateTime? startTime = null;
            string startTimeStr = Common.InputString("Start timestamp:", DateTime.Now.ToString(_TimestampFormat), true);
            if (!String.IsNullOrEmpty(startTimeStr)) startTime = Convert.ToDateTime(startTimeStr);

            node = _Topology.GetNodeById(nodeId);
            if (node == null)
            {
                Console.WriteLine("Unknown node");
                return;
            }

            string workerGuid = null;
            _ResyncMgr.Add(node, userGuid, containerName, startTime, out workerGuid);  
        }

        private void SyncTasks()
        {
            List<ResyncWorker> workers = _ResyncMgr.GetTasks();
            if (workers == null || workers.Count < 1)
            {
                Console.WriteLine("None");
            }
            else
            {
                Console.WriteLine(Common.SerializeJson(workers, true));
            }
        }

        private void SyncStart()
        {
            SyncTasks();
            string workerGuid = Common.InputString("Worker GUID:", null, true);
            if (String.IsNullOrEmpty(workerGuid)) return;

            _ResyncMgr.Start(workerGuid);
        }

        private RequestMetadata BuildMetadata(string userGuid, string container, string objectKey, WatsonWebserver.HttpMethod method)
        {
            RequestMetadata ret = new RequestMetadata();
            ret.Params = new RequestMetadata.Parameters();
            ret.Params.UserGuid = userGuid;
            ret.Params.Container = container;
            ret.Params.ObjectKey = objectKey;

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
