using System;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

using DatabaseWrapper;

using Kvpbase.Classes; 

namespace Kvpbase.Classes
{
    public class Settings
    {
        #region Public-Members-and-Nested-Classes
         
        public bool EnableConsole;
         
        public SettingsServer Server; 
        public SettingsStorage Storage;   
        public SettingsSyslog Syslog;
        public SettingsDatabase ConfigDatabase;
        public SettingsDatabase StorageDatabase;
         
        public class SettingsServer
        { 
            public int Port { get; set; } 
            public string DnsHostname { get; set; } 
            public bool Ssl { get; set; }

            public string HeaderApiKey;
            public string HeaderEmail;
            public string HeaderPassword;  
             
            public long MaxObjectSize;
            public int MaxTransferSize; 
        }
         
        public class SettingsStorage
        { 
            public string Directory; 
        }
            
        public class SettingsSyslog
        {
            public string ServerIp;
            public int ServerPort;
            public string Header;
            public int MinimumLevel;
            public bool LogHttpRequests;
            public bool LogHttpResponses;
            public bool ConsoleLogging;
        }
          
        public class SettingsDatabase
        {
            public DbTypes Type;  
            public string Hostname;
            public int Port;
            public string DatabaseName;
            public string InstanceName;
            public string Username;
            public string Password;
        }

        #endregion

        #region Constructors-and-Factories

        public Settings()
        {

        }

        public static Settings FromFile(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));
            if (!Common.FileExists(filename)) throw new FileNotFoundException(nameof(filename));

            string contents = Common.ReadTextFile(@filename); 
            Settings ret = Common.DeserializeJson<Settings>(contents);
            return ret;
        }

        #endregion
    }
}
