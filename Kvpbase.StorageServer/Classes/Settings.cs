using System;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

using DatabaseWrapper;

using Kvpbase.StorageServer.Classes; 

namespace Kvpbase.StorageServer.Classes
{
    /// <summary>
    /// Kvpbase settings.
    /// </summary>
    public class Settings
    {
        /// <summary>
        /// Enable or disable the console.
        /// </summary>
        public bool EnableConsole;
         
        /// <summary>
        /// Server settings.
        /// </summary>
        public SettingsServer Server; 

        /// <summary>
        /// Storage settings.
        /// </summary>
        public SettingsStorage Storage;   

        /// <summary>
        /// Syslog logging settings.
        /// </summary>
        public SettingsSyslog Syslog;

        /// <summary>
        /// Database settings.
        /// </summary>
        public SettingsDatabase Database;

        /// <summary>
        /// Debug settings.
        /// </summary>
        public SettingsDebug Debug;

        /// <summary>
        /// Server settings.
        /// </summary>
        public class SettingsServer
        { 
            /// <summary>
            /// TCP port on which to listen for HTTP requests.
            /// </summary>
            public int Port { get; set; } 

            /// <summary>
            /// DNS hostname on which to listen for HTTP requests.
            /// </summary>
            public string DnsHostname { get; set; } 

            /// <summary>
            /// Enable or disable SSL.
            /// </summary>
            public bool Ssl { get; set; }

            /// <summary>
            /// HTTP header to use for the API key.
            /// </summary>
            public string HeaderApiKey; 
             
            /// <summary>
            /// Maximum object size allowed.
            /// </summary>
            public long MaxObjectSize;

            /// <summary>
            /// Maximum transfer size allowed.
            /// </summary>
            public int MaxTransferSize; 
        }
         
        /// <summary>
        /// Storage settings.
        /// </summary>
        public class SettingsStorage
        { 
            /// <summary>
            /// Base directory for storage.
            /// </summary>
            public string Directory; 
        }
            
        /// <summary>
        /// Syslog logging settings.
        /// </summary>
        public class SettingsSyslog
        {
            /// <summary>
            /// Logging server IP address.
            /// </summary>
            public string ServerIp;

            /// <summary>
            /// Logging server syslog port.
            /// </summary>
            public int ServerPort;

            /// <summary>
            /// Header to prepend to every message.
            /// </summary>
            public string Header;

            /// <summary>
            /// Minimum logging level.
            /// </summary>
            public Severity MinimumLevel;

            /// <summary>
            /// Enable or disable console logging.
            /// </summary>
            public bool ConsoleLogging;

            /// <summary>
            /// Enable or disable file logging.
            /// </summary>
            public bool FileLogging;

            /// <summary>
            /// Log directory.
            /// </summary>
            public string LogDirectory;
        }
          
        /// <summary>
        /// Database settings.
        /// </summary>
        public class SettingsDatabase
        {
            /// <summary>
            /// Type of database.
            /// </summary>
            public DbTypes Type;

            /// <summary>
            /// For Sqlite databases, the filename.
            /// </summary>
            public string Filename;

            /// <summary>
            /// Hostname.
            /// </summary>
            public string Hostname;

            /// <summary>
            /// TCP port.
            /// </summary>
            public int Port;

            /// <summary>
            /// Database name.
            /// </summary>
            public string DatabaseName;

            /// <summary>
            /// For MS SQL Express, the instance name.
            /// </summary>
            public string InstanceName;

            /// <summary>
            /// Username.
            /// </summary>
            public string Username;

            /// <summary>
            /// Password.
            /// </summary>
            public string Password;
        }

        /// <summary>
        /// Debug settings.
        /// </summary>
        public class SettingsDebug
        {
            /// <summary>
            /// Database debugging.
            /// </summary>
            public bool Database;

            /// <summary>
            /// HTTP request logging.
            /// </summary>
            public bool HttpRequest; 
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public Settings()
        {

        }

        internal static Settings FromFile(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));
            if (!Common.FileExists(filename)) throw new FileNotFoundException(nameof(filename));

            string contents = Common.ReadTextFile(@filename); 
            Settings ret = Common.DeserializeJson<Settings>(contents);
            return ret;
        }
    }
}
