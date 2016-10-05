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
        public string DocumentationUrl;
        public string Environment;
        public string LogoUrl;
        public string HomepageUrl;
        public string SupportEmail;
        public int EnableConsole;

        public SettingsFiles Files;
        public SettingsServer Server;
        public SettingsRedirection Redirection;
        public SettingsTopology Topology;
        public SettingsPerfmon Perfmon;
        public SettingsStorage Storage;
        public SettingsMessages Messages;
        public SettingsExpiration Expiration;
        public SettingsReplication Replication;
        public SettingsBunker Bunker;
        public SettingsPublicObj PublicObj;
        public SettingsTasks Tasks;
        public SettingsLogger Logger;
        public SettingsSyslog Syslog;
        public SettingsEmail Email;
        public SettingsEncryption Encryption;
        public SettingsRest Rest;
        public SettingsMailgun Mailgun;
        public SettingsDebug Debug;

        public class SettingsFiles
        {
            public string Topology;
            public string UserMaster;
            public string ApiKey;
            public string Permission;
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
        }

        public class SettingsTopology
        {
            public int RefreshSec;
        }

        public class SettingsPerfmon
        {
            public int Enable;
            public int RefreshSec;
            public int Syslog;
        }

        public class SettingsStorage
        {
            public string Directory;
            public int MaxObjectSize;
            public int GatewayMode;
            public int DefaultCompress;
            public int DefaultEncrypt;
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

        public class SettingsBunker
        {
            public int Enable;
            public string Directory;
            public int RefreshSec;
            public List<BunkerNode> Nodes;
        }

        public class SettingsPublicObj
        {
            public string Directory;
            public int RefreshSec;
            public int DefaultExpirationSec;
        }

        public class BunkerNode
        {
            public int Enable;
            public string Name;
            public string Vendor;
            public string Version;
            public string UserGuid;
            public string Url;
            public string ApiKey;
        }

        public class SettingsTasks
        {
            public string Directory;
            public int RefreshSec;
        }

        public class SettingsLogger
        {
            public int RefreshSec;
        }

        public class SettingsRedirection
        {
            public string ReadRedirectionMode;
            public int ReadRedirectHttpStatus;
            public string ReadRedirectString;
            public string SearchRedirectionMode;
            public int SearchRedirectHttpStatus;
            public string SearchRedirectString;
            public string WriteRedirectionMode;
            public int WriteRedirectHttpStatus;
            public string WriteRedirectString;
            public string DeleteRedirectionMode;
            public int DeleteRedirectHttpStatus;
            public string DeleteRedirectString;
        }

        public class SettingsSyslog
        {
            public string ServerIp;
            public int ServerPort;
            public string Header;
            public int MinimumLevel;
            public int LogHttpRequests;
            public int LogHttpResponses;
            public int ConsoleLogging;
        }

        public class SettingsEmail
        {
            public string EmailProvider;
            public string SmtpServer;
            public int SmtpPort;
            public string SmtpUsername;
            public string SmtpPassword;
            public int SmtpSsl;

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
            public int Ssl;
            public string ApiKeyHeader;
            public string ApiKeyValue;

            public string Passphrase;
            public string Salt;
            public string Iv;
        }

        public class SettingsRest
        {
            public int UseWebProxy;
            public string WebProxyUrl;
            public int AcceptInvalidCerts;
        }

        public class SettingsMailgun
        {
            public string ApiKey;
            public string Domain;
        }

        public class SettingsDebug
        {
            public int DebugCompression;
            public int DebugEncryption;
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
            
            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("Reading system.json from local directory");
            string contents = Common.ReadTextFile(@"system.json");

            if (String.IsNullOrEmpty(contents))
            {
                Common.ExitApplication("Settings", "Unable to read contents of system.json", -1);
                return null;
            }

            Console.WriteLine("Deserializing system.json");
            Settings ret = null;

            try
            {
                ret = Common.DeserializeJson<Settings>(contents);
                if (ret == null)
                {
                    Common.ExitApplication("Settings", "Unable to deserialize system.json (null)", -1);
                    return null;
                }
            }
            catch (Exception e)
            {
                Events.ExceptionConsole("Settings", "Deserialization issue with system.json", e);
                Common.ExitApplication("Settings", "Unable to deserialize system.json (exception)", -1);
                return null;
            }

            return ret;
        }

        #endregion
    }
}
