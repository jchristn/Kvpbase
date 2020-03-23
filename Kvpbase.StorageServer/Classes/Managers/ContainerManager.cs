using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Timers;
using System.Threading.Tasks;
using DatabaseWrapper;
using SyslogLogging;
using Kvpbase.StorageServer.Classes.DatabaseObjects;
 
namespace Kvpbase.StorageServer.Classes.Managers
{
    internal class ContainerManager
    { 
        internal int ContainerRecheckIntervalSeconds
        {
            get
            {
                return _ContainerRecheckIntervalSeconds;
            }
            set
            {
                if (value <= 0) throw new ArgumentException("ContainerRecheckIntervalSeconds must be greater than 0.");
                _ContainerRecheckIntervalSeconds = value;
            }
        }
         
        private int _ContainerRecheckIntervalSeconds = 5;
        private Timer _ContainerRecheck; 
        private Settings _Settings;
        private LoggingModule _Logging;
        private DatabaseManager _Database;
        // private string _Header = "[Kvpbase.ContainerManager] ";
        private readonly object _ContainersLock;
        private List<ContainerClient> _ContainerClients;

        internal ContainerManager(Settings settings, LoggingModule logging, DatabaseManager databaseMgr)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (databaseMgr == null) throw new ArgumentNullException(nameof(databaseMgr)); 

            _Settings = settings;
            _Logging = logging;
            _Database = databaseMgr; 

            _ContainersLock = new object();
            _ContainerClients = new List<ContainerClient>();

            _ContainerRecheck = new Timer();
            _ContainerRecheck.Elapsed += new ElapsedEventHandler(RecheckContainers);
            _ContainerRecheck.Interval = (_ContainerRecheckIntervalSeconds * 1000);
            _ContainerRecheck.Enabled = true;

            InitializeContainerClients();
        }

        internal Container GetContainer(string userGuid, string name)
        { 
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            Expression e = new Expression("userguid", Operators.Equals, userGuid);
            e.PrependAnd("name", Operators.Equals, name);
            return _Database.SelectByFilter<Container>(e, "ORDER BY id DESC");
        }

        internal List<Container> GetContainers()
        {
            return _Database.SelectMany<Container>(null, null, null, "ORDER BY id DESC");
        }

        internal List<Container> GetContainersByUser(string userGuid)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            Expression e = new Expression("userguid", Operators.Equals, userGuid);
            return _Database.SelectMany<Container>(null, null, e, "ORDER BY id DESC");
        }

        internal bool GetContainerClient(string userGuid, string name, out ContainerClient client)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            lock (_ContainersLock)
            {
                client = _ContainerClients.Where(c => c.Container.UserGUID.Equals(userGuid) && c.Container.Name.Equals(name)).FirstOrDefault();
                if (client != null && client != default(ContainerClient)) return true;
            }

            Expression e = new Expression("name", Operators.Equals, name);
            e.PrependAnd("userguid", Operators.Equals, userGuid);
            Container container = _Database.SelectByFilter<Container>(e, "ORDER BY id DESC");
            if (container != null)
            {
                client = InitializeContainerClient(container);
                if (client != null) return true;
            }

            return false;
        }

        internal bool Add(Container container)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(container.Name)) return false;
            if (String.IsNullOrEmpty(container.UserGUID)) return false;
            if (String.IsNullOrEmpty(container.GUID)) return false;

            if (String.IsNullOrEmpty(container.ObjectsDirectory))
            {
                container.ObjectsDirectory =
                    _Settings.Storage.Directory +
                    container.UserGUID + "/" +
                    container.GUID + "/";
            }

            container = _Database.Insert<Container>(container);
            if (container == null) return false;
            InitializeContainerClient(container);
            return true;
        }

        internal bool Exists(string userGuid, string name)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            Expression e = new Expression("userguid", Operators.Equals, userGuid);
            e.PrependAnd(new Expression("name", Operators.Equals, name));
            Container container = _Database.SelectByFilter<Container>(e, "ORDER BY id DESC");
            if (container != null) return true;
            return false;
        }

        internal void Delete(string userGuid, string name, bool cleanup)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            ContainerClient client = null;
            if (GetContainerClient(userGuid, name, out client))
            {
                Expression e = new Expression("userguid", Operators.Equals, userGuid);
                e.PrependAnd("name", Operators.Equals, name);

                Container container = _Database.SelectByFilter<Container>(e, "ORDER BY id DESC");
                if (container != null) _Database.Delete<Container>(container);

                if (cleanup) client.Destroy();
                else client.Dispose();

                lock (_ContainersLock)
                {
                    if (_ContainerClients.Contains(client)) _ContainerClients.Remove(client);
                }
            }
        }
         
        private void InitializeContainerClients()
        {
            List<Container> containers = GetContainers(); 
            if (containers != null && containers.Count > 0)
            {
                foreach (Container curr in containers) InitializeContainerClient(curr);
            }
        }

        private ContainerClient InitializeContainerClient(Container container)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));

            lock (_ContainersLock)
            {
                if (_ContainerClients.Exists(c => c.Container.GUID.Equals(container.GUID)))
                {
                    ContainerClient remove = _ContainerClients.First(c => c.Container.GUID.Equals(container.GUID));
                    _ContainerClients.Remove(remove);
                }

                ContainerClient client = new ContainerClient(_Settings, _Logging, _Database, container);
                _ContainerClients.Add(client);
                return client;
            } 
        }

        private void RecheckContainers(object sender, ElapsedEventArgs e)
        {
            lock (_ContainersLock)
            {
                List<Container> containers = GetContainers();

                // evaluate for new containers
                foreach (Container curr in containers)
                {
                    if (!_ContainerClients.Exists(c => c.Container.GUID.Equals(curr.GUID)))
                    {
                        // new container found in the database
                        ContainerClient client = new ContainerClient(_Settings, _Logging, _Database, curr);
                        _ContainerClients.Add(client);
                    }
                }

                // evaluate 
                foreach (ContainerClient curr in _ContainerClients)
                {
                    if (!containers.Exists(c => c.GUID.Equals(curr.Container.GUID)))
                    {
                        // container removed
                        _ContainerClients.Remove(curr);
                        curr.Dispose();
                    }
                }
            }
        } 
    }
}
