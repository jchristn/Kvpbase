using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Timers;
using System.Threading.Tasks; 
using SyslogLogging;
using Watson.ORM;
using Watson.ORM.Core;
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
        private WatsonORM _ORM;
        // private string _Header = "[Kvpbase.ContainerManager] ";
        private readonly object _ContainersLock;
        private List<ContainerClient> _ContainerClients;

        internal ContainerManager(Settings settings, LoggingModule logging, WatsonORM orm)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (orm == null) throw new ArgumentNullException(nameof(orm)); 

            _Settings = settings;
            _Logging = logging;
            _ORM = orm; 

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

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<Container>(nameof(Container.UserGUID)),
                DbOperators.Equals,
                userGuid);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<Container>(nameof(Container.Name)),
                DbOperators.Equals,
                name));

            return _ORM.SelectFirst<Container>(e);
        }

        internal List<Container> GetContainers()
        {
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<Container>(nameof(Container.Id)),
                DbOperators.GreaterThan,
                0);

            return _ORM.SelectMany<Container>(e);
        }

        internal List<Container> GetContainersByUser(string userGuid)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<Container>(nameof(Container.UserGUID)),
                DbOperators.Equals,
                userGuid);

            return _ORM.SelectMany<Container>(e);
        }

        internal ContainerClient GetContainerClient(string userGuid, string name)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            ContainerClient client = null;

            lock (_ContainersLock)
            {
                client = _ContainerClients.Where(c => c.Container.UserGUID.Equals(userGuid) && c.Container.Name.Equals(name)).FirstOrDefault();
                if (client != null && client != default(ContainerClient)) return client;
            }

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<Container>(nameof(Container.UserGUID)),
                DbOperators.Equals,
                userGuid);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<Container>(nameof(Container.Name)),
                DbOperators.Equals,
                name));

            Container container = _ORM.SelectFirst<Container>(e);
            if (container != null)
            {
                client = InitializeContainerClient(container);
                if (client != null) return client;
            }

            return null;
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

            container = _ORM.Insert<Container>(container);
            if (container == null) return false;
            InitializeContainerClient(container);
            return true;
        }

        internal bool Exists(string userGuid, string name)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<Container>(nameof(Container.UserGUID)),
                DbOperators.Equals,
                userGuid);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<Container>(nameof(Container.Name)),
                DbOperators.Equals,
                name));
 
            Container container = _ORM.SelectFirst<Container>(e);
            if (container != null) return true;
            return false;
        }

        internal void Update(Container container)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));  
            Delete(container.UserGUID, container.Name, false); 
            Add(container);
        }

        internal void Delete(string userGuid, string name, bool cleanup)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            ContainerClient client = GetContainerClient(userGuid, name);
            if (client != null)
            {
                DbExpression e = new DbExpression(
                    _ORM.GetColumnName<Container>(nameof(Container.UserGUID)),
                    DbOperators.Equals,
                    userGuid);

                e.PrependAnd(new DbExpression(
                    _ORM.GetColumnName<Container>(nameof(Container.Name)),
                    DbOperators.Equals,
                    name));
                 
                Container container = _ORM.SelectFirst<Container>(e);
                if (container != null) _ORM.Delete<Container>(container);

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
                foreach (Container curr in containers)
                {
                    InitializeContainerClient(curr);
                }
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

                ContainerClient client = new ContainerClient(_Settings, _Logging, _ORM, container);
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
                        ContainerClient client = new ContainerClient(_Settings, _Logging, _ORM, curr);
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
