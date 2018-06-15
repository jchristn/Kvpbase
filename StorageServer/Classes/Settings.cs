using System;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

namespace Kvpbase
{
    public class Settings
    {
        #region Public-Members-and-Nested-Classes

        public string ProductName;
        public string ProductVersion; 
        public string Environment; 
        public string HomepageUrl;
        public string SupportEmail;
        public bool EnableConsole;

        public SettingsFiles Files;
        public SettingsServer Server; 
        public SettingsRedirection Redirection;
        public SettingsTopology Topology; 
        public SettingsStorage Storage;
        public SettingsContainer Container;
        public SettingsMessages Messages;
        public SettingsExpiration Expiration;
        public SettingsReplication Replication; 
        public SettingsTasks Tasks; 
        public SettingsSyslog Syslog;
        public SettingsEmail Email;
        public SettingsEncryption Encryption;
        public SettingsRest Rest;
        public SettingsMailgun Mailgun; 

        public class SettingsFiles
        {
            public string Topology;
            public string UserMaster;
            public string ApiKey;
            public string Permission;
            public string Container;
        }

        public class SettingsServer
        {
            public string HeaderApiKey;
            public string HeaderEmail;
            public string HeaderPassword;
            public string HeaderToken;
            public string HeaderVersion;

            public string AdminApiKey;
            public int TokenExpirationSec;
            public int FailedRequestsIntervalSec;
            public int MaxTransferSize;
        }

        public class SettingsTopology
        { 
            public bool DebugMeshNetworking;
            public bool DebugMessages;
        }

        public class SettingsStorage
        {
            public string Directory; 
        }

        public class SettingsContainer
        {
            public int CacheSize;
            public int EvictSize;
            public ReplicationMode DefaultReplicationMode;
        }

        public class SettingsMessages
        {
            public string Directory;
            public int RefreshSec;
        }

        public class SettingsExpiration
        {
            public string Directory;
            public int RefreshSec;
            public int DefaultExpirationSec;
        }

        public class SettingsReplication
        {
            public string Directory;
            public string ReplicationMode;
            public int RefreshSec;
        }
         
        public class SettingsTasks
        {
            public string Directory;
            public int RefreshSec;
        }
 
        public class SettingsRedirection
        {
            public RedirectMode Mode; 
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

        public class SettingsEmail
        {
            public string EmailProvider;
            public string SmtpServer;
            public int SmtpPort;
            public string SmtpUsername;
            public string SmtpPassword;
            public bool SmtpSsl;

            public int EmailExceptions;
            public string EmailExceptionsTo;
            public string EmailExceptionsFrom;
            public string EmailExceptionsReplyTo;

            public string EmailFrom;
            public string EmailReplyTo;
        }

        public class SettingsEncryption
        {
            public string Mode;

            public string Server;
            public int Port;
            public bool Ssl;
            public string ApiKeyHeader;
            public string ApiKeyValue;

            public string Passphrase;
            public string Salt;
            public string Iv;
        }

        public class SettingsRest
        {
            public bool UseWebProxy;
            public string WebProxyUrl;
            public bool AcceptInvalidCerts;
        }

        public class SettingsMailgun
        {
            public string ApiKey;
            public string Domain;
            public string ResourceSendmessage;
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
            if (String.IsNullOrEmpty(contents))
            {
                Common.ExitApplication("Settings", "Unable to read contents of " + filename, -1);
                return null;
            }
             
            Settings ret = null;

            try
            {
                ret = Common.DeserializeJson<Settings>(contents);
                if (ret == null)
                {
                    Common.ExitApplication("Settings", "Unable to deserialize " + filename + " (null)", -1);
                    return null;
                }
            }
            catch (Exception)
            { 
                Common.ExitApplication("Settings", "Unable to deserialize " + filename + " (exception)", -1);
                return null;
            }

            return ret;
        }

        #endregion
    }
}
