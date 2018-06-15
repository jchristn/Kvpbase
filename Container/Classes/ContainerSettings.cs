using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kvpbase
{
    /// <summary>
    /// Settings for a container.
    /// </summary>
    public class ContainerSettings
    {
        #region Public-Members

        /// <summary>
        /// The username of the ontainer owner.
        /// </summary>
        public string User { get; set; }

        /// <summary>
        /// The name of the container.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The root directory of the container on the file system.
        /// </summary>
        public string RootDirectory { get; set; }

        /// <summary>
        /// The full path and filename of the container's metadata database.
        /// </summary>
        public string DatabaseFilename { get; set; }

        /// <summary>
        /// The full path to where container objects should be stored.
        /// </summary>
        public string ObjectsDirectory { get; set; }

        /// <summary>
        /// The type of object handler used by the container.
        /// </summary>
        public ObjectHandlerType HandlerType { get; set; }

        /// <summary>
        /// Enable or disable audit logging.
        /// </summary>
        public bool EnableAuditLogging { get; set; }

        /// <summary>
        /// Enable or disable public read access.
        /// </summary>
        public bool IsPublicRead { get; set; }

        /// <summary>
        /// Enable or disable public write access.
        /// </summary>
        public bool IsPublicWrite { get; set; } 

        /// <summary>
        /// Enable or disable database query debug logging.
        /// </summary>
        public bool DatabaseDebug { get; set; }

        /// <summary>
        /// The replication mode for the container.
        /// </summary>
        public ReplicationMode Replication { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object using default settings.
        /// </summary>
        public ContainerSettings()
        {
            DefaultSettings("Default", "Default", "./");
        }

        /// <summary>
        /// Instantiate the object with a specific name (propagates to child members).
        /// </summary>
        /// <param name="user">The name of the user that owns the container.</param>
        /// <param name="name">The name of the container.</param>
        /// <param name="baseDir">The base directory under which the container subdirectory will be created.</param>
        public ContainerSettings(string user, string name, string baseDir)
        {
            if (String.IsNullOrEmpty(user)) throw new ArgumentNullException(nameof(user));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            if (String.IsNullOrEmpty(baseDir)) throw new ArgumentNullException(nameof(baseDir));
            DefaultSettings(user, name, baseDir);
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        private void DefaultSettings(string user, string name, string baseDir)
        {
            if (String.IsNullOrEmpty(user)) user = "Default";
            if (String.IsNullOrEmpty(name)) name = "Default";
            if (String.IsNullOrEmpty(baseDir)) baseDir = "./";

            if (!baseDir.EndsWith("/")) baseDir += "/";

            User = user;
            Name = name;
            RootDirectory = baseDir + name + "/";
            DatabaseFilename = RootDirectory + "__Container__.db";
            ObjectsDirectory = RootDirectory + "__Objects__/";
            HandlerType = ObjectHandlerType.Disk;
            EnableAuditLogging = false;
            IsPublicRead = true;
            IsPublicWrite = false;
            DatabaseDebug = false;
            Replication = ReplicationMode.None;
        }

        #endregion
    }
}
