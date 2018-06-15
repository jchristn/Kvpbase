using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Caching;

namespace Kvpbase
{
    /// <summary>
    /// Container manager with caching.
    /// </summary>
    public class ContainerManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private string _ContainersFile;

        private readonly object _ContainerCreateLock;
        private readonly object _ContainersFileLock;
        private readonly object _ContainersLock;
        private List<ContainerSettings> _Containers;

        private LRUCache<Container> _Cache;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="containersFile">File containing JSON of all of the containers.</param>
        /// <param name="cacheSize">Number of container objects to cache in RAM.</param>
        /// <param name="evictSize">Number of container objects to evict when cache becomes full.</param>
        public ContainerManager(string containersFile, int cacheSize, int evictSize)
        {
            if (String.IsNullOrEmpty(containersFile)) throw new ArgumentNullException(nameof(containersFile));
            if (cacheSize < 1) throw new ArgumentException("Cache size must be one or greater");
            if (evictSize >= cacheSize) throw new ArgumentException("Evict size must be less than cache size");

            _ContainerCreateLock = new object();
            _ContainersFileLock = new object();
            _ContainersFile = containersFile;
            _ContainersLock = new object();
            ReadContainersFile();

            _Cache = new LRUCache<Container>(cacheSize, evictSize, false);
            
            InitializeContainers();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Retrieve a container object.
        /// </summary>
        /// <param name="user">The user that owns the container.</param>
        /// <param name="name">The name of the container.</param>
        /// <param name="container">Container.</param>
        /// <returns>True if found.</returns>
        public bool GetContainer(string user, string name, out Container container)
        {
            container = null;
            if (String.IsNullOrEmpty(user)) throw new ArgumentNullException(nameof(user));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            if (!Exists(user, name)) return false;

            if (_Cache.TryGet(CacheEntryName(user.ToLower(), name.ToLower()), out container))
            {
                return true;
            }
            else
            {
                // not in cache
                lock (_ContainersLock)
                {
                    ContainerSettings currSettings = _Containers.Where(c => c.Name.ToLower().Equals(name.ToLower()) && c.User.ToLower().Equals(user.ToLower())).FirstOrDefault();
                    if (currSettings == null || currSettings == default(ContainerSettings))
                    {
                        container = null;
                        return false;
                    }

                    lock (_ContainerCreateLock)
                    {
                        container = new Container(currSettings);
                    }

                    _Cache.AddReplace(CacheEntryName(user.ToLower(), name.ToLower()), container);
                    return true;
                }
            } 
        }

        /// <summary>
        /// Retrieve a list of all container.
        /// </summary>
        /// <param name="settings"></param>
        /// <returns></returns>
        public bool GetContainers(out List<ContainerSettings> settings)
        {
            settings = new List<ContainerSettings>();

            lock (_ContainersLock)
            {
                settings = new List<ContainerSettings>(_Containers);
                return true;
            }
        }

        /// <summary>
        /// Retrieve a container's settings.
        /// </summary>
        /// <param name="user">The user that owns the container.</param>
        /// <param name="name">The name of the container.</param>
        /// <param name="settings">ContainerSettings.</param>
        /// <returns>True if found.</returns>
        public bool GetContainerSettings(string user, string name, out ContainerSettings settings)
        {
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            lock (_ContainersLock)
            {
                settings = _Containers.Where(c => c.Name.ToLower().Equals(name.ToLower()) && c.User.ToLower().Equals(user.ToLower())).FirstOrDefault();
                if (settings == null || settings == default(ContainerSettings))
                {
                    settings = null;
                    return false;
                }

                return true;
            }
        }

        /// <summary>
        /// Retrieve containers owned by a given user.
        /// </summary>
        /// <param name="user">The user that owns the containers.</param>
        /// <param name="containers">List of ContainerSettings.</param>
        /// <returns>True if successful.</returns>
        public bool GetContainersByUser(string user, out List<string> containers)
        {
            if (String.IsNullOrEmpty(user)) throw new ArgumentNullException(nameof(user));

            containers = new List<string>(); 
            List<ContainerSettings> containerSettings = new List<ContainerSettings>();

            lock (_ContainersLock)
            {
                containerSettings = _Containers.Where(c => c.User.ToLower().Equals(user.ToLower())).ToList();
            }

            if (containerSettings != null)
            {
                foreach (ContainerSettings currSettings in containerSettings)
                {
                    containers.Add(currSettings.Name.ToLower());
                }
            }

            return true;
        }

        /// <summary>
        /// Add a container.
        /// </summary>
        /// <param name="settings">ContainerSettings.</param>
        public void Add(ContainerSettings settings)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            
            lock (_Containers)
            {
                ContainerSettings tempSettings = _Containers.Where(c => c.Name.ToLower().Equals(settings.Name.ToLower()) && c.User.ToLower().Equals(settings.User.ToLower())).FirstOrDefault();
                if (tempSettings != null && tempSettings != default(ContainerSettings))
                {
                    _Containers.Remove(tempSettings);
                }

                _Containers.Add(settings);
                WriteContainersFile();
            }

            Container currContainer = null;

            lock (_ContainerCreateLock)
            {
                currContainer = new Container(settings);
            }

            currContainer.ApplyContainerSettings();

            _Cache.AddReplace(CacheEntryName(settings.User.ToLower(), settings.Name.ToLower()), currContainer);
            return;
        }

        /// <summary>
        /// Check if a container exists.
        /// </summary>
        /// <param name="user">The user that owns the container.</param>
        /// <param name="name">The name of the container.</param>
        /// <returns>True if exists.</returns>
        public bool Exists(string user, string name)
        {
            if (String.IsNullOrEmpty(user)) throw new ArgumentNullException(nameof(user));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            lock (_ContainersLock)
            {
                return _Containers.Any(c => c.Name.ToLower().Equals(name.ToLower()) && c.User.ToLower().Equals(user.ToLower()));
            }
        }

        /// <summary>
        /// Delete a container.
        /// </summary>
        /// <param name="user">The user that owns the container.</param>
        /// <param name="name">The name of the container.</param>
        /// <param name="cleanup">True if you wish to destroy the container and its data.</param>
        public void Delete(string user, string name, bool cleanup)
        {
            if (String.IsNullOrEmpty(user)) throw new ArgumentNullException(nameof(user));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            // dispose or destroy
            Container currContainer = null;
            GetContainer(user, name, out currContainer);
            if (currContainer != null && currContainer != default(Container))
            {
                if (cleanup) currContainer.Destroy();
                else currContainer.Dispose();
            }

            // remove from list
            lock (_ContainersLock)
            {
                List<ContainerSettings> remove = _Containers.Where(c => c.User.ToLower().Equals(user.ToLower()) && c.Name.ToLower().Equals(name.ToLower())).ToList();

                foreach (ContainerSettings currSettings in remove)
                {
                    _Containers.Remove(currSettings);
                }

                WriteContainersFile();
                _Cache.Remove(CacheEntryName(user, name));
            }
        }

        /// <summary>
        /// Retrieve list of cached container objects.
        /// </summary>
        /// <returns>List of container names.</returns>
        public List<string> CachedContainers()
        {
            return _Cache.GetKeys();
        }

        #endregion

        #region Private-Methods

        private void ReadContainersFile()
        {
            lock (_ContainersFileLock)
            {
                if (!File.Exists(_ContainersFile))
                {
                    File.WriteAllBytes(_ContainersFile, Encoding.UTF8.GetBytes("[ ]"));
                }

                _Containers = Common.DeserializeJson<List<ContainerSettings>>(File.ReadAllBytes(_ContainersFile));
            }
        }

        private void WriteContainersFile()
        {
            lock (_ContainersFileLock)
            {
                File.WriteAllBytes(_ContainersFile, Encoding.UTF8.GetBytes(Common.SerializeJson(_Containers, true)));
            }
        }
         
        private void InitializeContainers()
        {
            List<ContainerSettings> settings = null;

            lock (_ContainersLock)
            {
                settings = new List<ContainerSettings>(_Containers);
            }

            if (settings == null || settings.Count < 1) return;

            foreach (ContainerSettings currSettings in settings)
            {
                Container currContainer = null;
                if (!GetContainer(currSettings.User, currSettings.Name, out currContainer))
                {
                    continue;
                }

                currContainer.ApplyContainerSettings();
            }
        }

        private string CacheEntryName(string user, string name)
        {
            return user.ToLower() + "-" + name.ToLower();
        }

        #endregion
    }
}
