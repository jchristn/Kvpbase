using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Timers;
using System.Threading.Tasks;

using Kvpbase.Containers;

using DatabaseWrapper;
using SyslogLogging;
 
namespace Kvpbase.Classes.Managers
{ 
    public class ContainerManager
    {
        #region Public-Members

        public int ContainerRecheckIntervalSeconds
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

        #endregion

        #region Private-Members

        private int _ContainerRecheckIntervalSeconds = 5;
        private Timer _ContainerRecheck;

        private Settings _Settings;
        private LoggingModule _Logging;
        private ConfigManager _Config;
        private DatabaseClient _Database;

        private readonly object _ContainersLock;
        private List<ContainerClient> _ContainerClients;
         
        #endregion

        #region Constructors-and-Factories
         
        public ContainerManager(Settings settings, LoggingModule logging, ConfigManager config, DatabaseClient database)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (database == null) throw new ArgumentNullException(nameof(database));

            _Settings = settings;
            _Logging = logging;
            _Config = config;
            _Database = database;

            _ContainersLock = new object();
            _ContainerClients = new List<ContainerClient>();

            _ContainerRecheck = new Timer();
            _ContainerRecheck.Elapsed += new ElapsedEventHandler(RecheckContainers);
            _ContainerRecheck.Interval = (_ContainerRecheckIntervalSeconds * 1000);
            _ContainerRecheck.Enabled = true;

            InitializeContainerClients();
        }

        #endregion

        #region Public-Methods
         
        public Container GetContainer(string userGuid, string name)
        { 
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            return _Config.GetContainer(userGuid, name);
        }
         
        public List<Container> GetContainers()
        {
            return _Config.GetContainers();
        }
          
        public List<Container> GetContainersByUser(string userGuid)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            return _Config.GetContainersByUser(userGuid); 
        }
         
        public bool GetContainerClient(string userGuid, string name, out ContainerClient client)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            lock (_ContainersLock)
            {
                client = _ContainerClients.Where(c => c.Container.UserGuid.Equals(userGuid) && c.Container.Name.Equals(name)).FirstOrDefault();
                if (client != null && client != default(ContainerClient)) return true;
            }

            Container container = _Config.GetContainer(userGuid, name);
            if (container != null)
            {
                client = InitializeContainerClient(container);
                if (client != null) return true;
            }

            return false;
        }
         
        public bool Add(Container container)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(container.Name)) return false;
            if (String.IsNullOrEmpty(container.UserGuid)) return false;
            if (String.IsNullOrEmpty(container.GUID)) return false;
             
            bool success = _Config.AddContainer(container);
            if (success) InitializeContainerClient(container);
            return success;
        }
         
        public bool Exists(string userGuid, string name)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            return _Config.ContainerExists(userGuid, name);
        }
         
        public void Delete(string userGuid, string name, bool cleanup)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            ContainerClient client = null;
            if (GetContainerClient(userGuid, name, out client))
            {
                _Config.RemoveContainer(client.Container);

                if (cleanup) client.Destroy();
                else client.Dispose();

                lock (_ContainersLock)
                {
                    if (_ContainerClients.Contains(client)) _ContainerClients.Remove(client);
                }
            }
        }
        
        #endregion

        #region Private-Methods
           
        private void InitializeContainerClients()
        {
            List<Container> containers = _Config.GetContainers();

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

                ContainerClient client = new ContainerClient(_Settings, _Logging, _Config, _Database, container);
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
                        ContainerClient client = new ContainerClient(_Settings, _Logging, _Config, _Database, curr);
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

        #endregion
    }
}
