using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using SyslogLogging;

using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase
{
    public class Setup
    {
        #region Public-Members

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public Setup()
        {
            RunSetup();
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        private void RunSetup()
        {
            #region Variables

            DateTime timestamp = DateTime.Now;
            Settings currSettings = new Settings(); 

            ApiKey currApiKey = new ApiKey();
            List<ApiKey> apiKeys = new List<ApiKey>();

            ApiKeyPermission currPerm = new ApiKeyPermission();
            List<ApiKeyPermission> permissions = new List<ApiKeyPermission>();

            UserMaster currUser = new UserMaster();
            List<UserMaster> users = new List<UserMaster>();

            Topology currTopology = new Topology();
            Node currNode = new Node();

            #endregion

            #region Welcome

            Console.WriteLine("");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine(@"   _             _                    ");
            Console.WriteLine(@"  | |____ ___ __| |__  __ _ ___ ___   ");
            Console.WriteLine(@"  | / /\ V / '_ \ '_ \/ _` (_-</ -_)  ");
            Console.WriteLine(@"  |_\_\ \_/| .__/_.__/\__,_/__/\___|  ");
            Console.WriteLine(@"           |_|                        ");
            Console.WriteLine(@"                                      ");
            Console.ResetColor();

            Console.WriteLine("");
            Console.WriteLine("Kvpbase Storage Server");
            Console.WriteLine("");
            //          1         2         3         4         5         6         7
            // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("Thank you for using Kvpbase!  We'll put together a basic system configuration");
            Console.WriteLine("so you can be up and running quickly.  You'll want to modify the System.json");
            Console.WriteLine("file after to ensure a more secure operating environment.");
            Console.WriteLine("");
            Console.WriteLine("Press ENTER to get started.");
            Console.WriteLine("");
            Console.WriteLine(Common.Line(79, "-"));
            Console.ReadLine();

            #endregion

            #region Initial-Settings

            currSettings.ProductName = "Kvpbase";   
            currSettings.EnableConsole = true;
             
            currSettings.Files = new Settings.SettingsFiles();
            currSettings.Files.ApiKey = "./ApiKey.json";
            currSettings.Files.Permission = "./ApiKeyPermission.json";
            currSettings.Files.Topology = "./Topology.json";
            currSettings.Files.UserMaster = "./UserMaster.json";
            currSettings.Files.Container = "./Container.json";
             
            currSettings.Server = new Settings.SettingsServer();
            currSettings.Server.HeaderApiKey = "x-api-key";
            currSettings.Server.HeaderEmail = "x-email";
            currSettings.Server.HeaderPassword = "x-password";
            currSettings.Server.HeaderToken = "x-token";
            currSettings.Server.HeaderVersion = "x-version";
            currSettings.Server.AdminApiKey = "kvpbaseadmin";
            currSettings.Server.TokenExpirationSec = 86400;
            currSettings.Server.FailedRequestsIntervalSec = 60;
            currSettings.Server.MaxObjectSize = 2199023255552;      // 2TB object size
            currSettings.Server.MaxTransferSize = 536870912;        // 512MB transfer size

            currSettings.Redirection = new Settings.SettingsRedirection();
            currSettings.Redirection.Mode = RedirectMode.PermanentRedirect; 
             
            currSettings.Topology = new Settings.SettingsTopology(); 
            currSettings.Topology.DebugMeshNetworking = false;
            currSettings.Topology.DebugMeshNetworking = false; 

            currSettings.Storage = new Settings.SettingsStorage();
            currSettings.Storage.TempFiles = "./Temp/";
            currSettings.Storage.Directory = "./Storage/"; 
             
            currSettings.Container = new Settings.SettingsContainer();
            currSettings.Container.CacheSize = 100;
            currSettings.Container.EvictSize = 10;
            currSettings.Container.DefaultReplicationMode = ReplicationMode.Async;

            currSettings.Messages = new Settings.SettingsMessages();
            currSettings.Messages.Directory = "./Messages/";
            currSettings.Messages.RefreshSec = 10;
             
            currSettings.Expiration = new Settings.SettingsExpiration();
            currSettings.Expiration.Directory = "./Expiration/";
            currSettings.Expiration.RefreshSec = 10;
            currSettings.Expiration.DefaultExpirationSec = 0;
             
            currSettings.Replication = new Settings.SettingsReplication();
            currSettings.Replication.Directory = "./Replication/";
            currSettings.Replication.RefreshSec = 10;
            currSettings.Replication.ReplicationMode = "sync";
             
            currSettings.Tasks = new Settings.SettingsTasks();
            currSettings.Tasks.Directory = "./Tasks/";
            currSettings.Tasks.RefreshSec = 10;
             
            currSettings.Syslog = new Settings.SettingsSyslog();
            currSettings.Syslog.ConsoleLogging = true;
            currSettings.Syslog.Header = "kvpbase";
            currSettings.Syslog.ServerIp = "127.0.0.1";
            currSettings.Syslog.ServerPort = 514;
            currSettings.Syslog.LogHttpRequests = false;
            currSettings.Syslog.LogHttpResponses = false;
            currSettings.Syslog.MinimumLevel = 1;
             
            currSettings.Email = new Settings.SettingsEmail();
             
            currSettings.Encryption = new Settings.SettingsEncryption();
            currSettings.Encryption.Mode = "local";
            currSettings.Encryption.Iv = "0000000000000000";
            currSettings.Encryption.Passphrase = "0000000000000000";
            currSettings.Encryption.Salt = "0000000000000000";
             
            currSettings.Rest = new Settings.SettingsRest();
            currSettings.Rest.AcceptInvalidCerts = true;
            currSettings.Rest.UseWebProxy = false;
             
            currSettings.Mailgun = new Settings.SettingsMailgun();
              
            if (!Common.WriteFile("System.json", Common.SerializeJson(currSettings, true), false))
            {
                Common.ExitApplication("setup", "Unable to write System.json", -1);
                return;
            } 

            #endregion

            #region Users-API-Keys-and-Permissions
             
            currApiKey = new ApiKey();
            currApiKey.Active = true;
            currApiKey.ApiKeyId = 1;
            currApiKey.Created = timestamp;
            currApiKey.LastUpdate = timestamp;
            currApiKey.Expiration = timestamp.AddYears(100);
            currApiKey.Guid = "default";
            currApiKey.Notes = "Created by setup script";
            currApiKey.UserMasterId = 1;
            apiKeys.Add(currApiKey);

            currPerm = new ApiKeyPermission();
            currPerm.Active = true;
            currPerm.DeleteContainer = true;
            currPerm.DeleteObject = true;
            currPerm.ReadContainer = true;
            currPerm.ReadObject = true; 
            currPerm.WriteContainer = true;
            currPerm.WriteObject = true;
            currPerm.ApiKeyId = 1;
            currPerm.ApiKeyPermissionId = 1;
            currPerm.Created = timestamp;
            currPerm.LastUpdate = timestamp;
            currPerm.Expiration = timestamp.AddYears(100);
            currPerm.Guid = "default";
            currPerm.Notes = "Created by setup script";
            currPerm.UserMasterId = 1;
            permissions.Add(currPerm);

            currUser = new UserMaster();
            currUser.Active = 1;
            currUser.Address1 = "123 Some Street";
            currUser.Cellphone = "408-555-1212";
            currUser.City = "San Jose";
            currUser.CompanyName = "Default Company";
            currUser.Country = "USA";
            currUser.FirstName = "First";
            currUser.LastName = "Last";
            currUser.Email = "default@default.com"; 
            currUser.NodeId = 0;
            currUser.Password = "default";
            currUser.PostalCode = "95128";
            currUser.State = "CA";
            currUser.UserMasterId = 1;
            currUser.Guid = "default";
            currUser.Created = timestamp;
            currUser.LastUpdate = timestamp;
            currUser.Expiration = timestamp.AddYears(100);
            users.Add(currUser);

            if (!Common.WriteFile(currSettings.Files.ApiKey, Common.SerializeJson(apiKeys, true), false))
            {
                Common.ExitApplication("Setup", "Unable to write " + currSettings.Files.ApiKey, -1);
                return;
            }

            if (!Common.WriteFile(currSettings.Files.Permission, Common.SerializeJson(permissions, true), false))
            {
                Common.ExitApplication("Setup", "Unable to write " + currSettings.Files.Permission, -1);
                return;
            }

            if (!Common.WriteFile(currSettings.Files.UserMaster, Common.SerializeJson(users, true), false))
            {
                Common.ExitApplication("Setup", "Unable to write " + currSettings.Files.UserMaster, -1);
                return;
            }

            Console.WriteLine("We have created your first user account and permissions.");
            Console.WriteLine("  Email    : " + currUser.Email);
            Console.WriteLine("  Password : " + currUser.Password);
            Console.WriteLine("  GUID     : " + currUser.Guid);
            Console.WriteLine("  API Key  : " + currApiKey.Guid);
            Console.WriteLine("");
            Console.WriteLine("This was done by creating the following files:");
            Console.WriteLine("  " + currSettings.Files.UserMaster);
            Console.WriteLine("  " + currSettings.Files.ApiKey);
            Console.WriteLine("  " + currSettings.Files.Permission);
            Console.WriteLine(""); 

            #endregion

            #region Topology
             
            currTopology = new Topology();
            currTopology.NodeId = 1;

            currTopology.Nodes = new List<Node>();
            currNode = new Node();
            currNode.NodeId = 1;
            currNode.Name = "localhost"; 
            currNode.NodeId = 1;

            currNode.Http = new Node.HttpSettings();
            currNode.Http.DnsHostname = "localhost";
            currNode.Http.Port = 8000;
            currNode.Http.Ssl = false;

            currNode.Tcp = new Node.TcpSettings();
            currNode.Tcp.IpAddress = "127.0.0.1";
            currNode.Tcp.Port = 9000;
            currNode.Tcp.Ssl = false;
            currNode.Tcp.Timeout = new Node.TcpSettings.TimeoutSettings();
            currNode.Tcp.Timeout.MinTimeoutSec = 10;
            currNode.Tcp.Timeout.MaxTimeoutSec = 30;
            currNode.Tcp.Timeout.ExpectedXferRateBytesPerSec = 10485760;

            //          1         2         3         4         5         6         7
            // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("IMPORTANT: Kvpbase can only receive requests on the hostname you set.");
            Console.WriteLine("If you set the hostname to 'localhost', this node will ONLY receive and handle");
            Console.WriteLine("requests destined to 'localhost', i.e. it will only handle local requests.");
            Console.WriteLine("");
            currNode.Http.DnsHostname = Common.InputString("On which hostname shall this node listen?", "localhost", false); 

            Console.WriteLine("");
            currNode.Http.Port = Common.InputInteger("HTTP port for API requests?", 8000, true, false);
            currNode.Tcp.Port = Common.InputInteger("TCP port for management requests?", 9000, true, false);
            Console.WriteLine("");

            currTopology.Nodes = new List<Node>();
            currTopology.Nodes.Add(currNode);
            currTopology.Replicas = null;

            if (!Common.WriteFile(currSettings.Files.Topology, Common.SerializeJson(currTopology, true), false))
            {
                Common.ExitApplication("Setup", "Unable to write " + currSettings.Files.Topology, -1);
                return;
            }

            //          1         2         3         4         5         6         7
            // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("We've created your topology file.  This node is configured to");
            Console.WriteLine("use HTTP (not HTTPS) and is accessible at the following URL:");
            Console.WriteLine("");
            Console.WriteLine("  http://" + currNode.Http.DnsHostname + ":" + currNode.Http.Port);
                 
            Console.WriteLine("");
            Console.WriteLine("Be sure to install an SSL certificate and modify your Topology.json file to");
            Console.WriteLine("use SSL to maximize security and set the correct hostname.");
            Console.WriteLine("");
            Console.WriteLine("Also, be sure to configure your firewall to allow inbound requests on both");
            Console.WriteLine("HTTP port " + currNode.Http.Port + " and TCP port " + currNode.Tcp.Port + ".");
            Console.WriteLine("");


            #endregion

            #region Create-Directories

            currSettings.Storage.Directory = "./Storage/";
            currSettings.Messages.Directory = "./Messages/";
            currSettings.Expiration.Directory = "./Expiration/";
            currSettings.Replication.Directory = "./Replication/"; 
            currSettings.Tasks.Directory = "./Tasks/";

            Directory.CreateDirectory(currSettings.Storage.Directory);
            Directory.CreateDirectory(currSettings.Messages.Directory);
            Directory.CreateDirectory(currSettings.Expiration.Directory);
            Directory.CreateDirectory(currSettings.Replication.Directory);
            Directory.CreateDirectory(currSettings.Tasks.Directory);

            #endregion

            #region Create-First-Container

            string htmlFile = SampleHtmlFile("http://github.com/kvpbase");
            string textFile = SampleTextFile("http://github.com/kvpbase");
            string jsonFile = SampleJsonFile("http://github.com/kvpbase");

            ContainerManager containerManager = new ContainerManager(currSettings.Files.Container, 10, 1);
            ContainerSettings containerSettings = new ContainerSettings("default", "default", currSettings.Storage.Directory + "default");
            containerSettings.EnableAuditLogging = true;
            containerManager.Add(containerSettings);

            Container defaultContainer = null;
            containerManager.GetContainer("default", "default", out defaultContainer);

            ErrorCode error;
            defaultContainer.WriteObject("hello.html", "text/html", Encoding.UTF8.GetBytes(htmlFile), null, out error);
            defaultContainer.WriteObject("hello.txt", "text/plain", Encoding.UTF8.GetBytes(textFile), null, out error);
            defaultContainer.WriteObject("hello.json", "application/json", Encoding.UTF8.GetBytes(jsonFile), null, out error);

            #endregion
             
            #region Wrap-Up

            //          1         2         3         4         5         6         7
            // 12345678901234567890123456789012345678901234567890123456789012345678901234567890

            Console.WriteLine("");
            Console.WriteLine("All finished!");
            Console.WriteLine("");
            Console.WriteLine("If you ever want to return to this setup wizard, just re-run the application");
            Console.WriteLine("from the terminal with the 'setup' argument.");
            Console.WriteLine("");

            Console.WriteLine("Press ENTER to start.");
            Console.WriteLine("");
            Console.ReadLine();

            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("");
            Console.WriteLine("We created a couple of sample files for you so that you can see your node in");
            Console.WriteLine("action.  Go to the following URLs in your browser and see what happens!");
            Console.WriteLine("");
            Console.WriteLine("  http://" + currNode.Http.DnsHostname + ":" + currNode.Http.Port + "/");
            Console.WriteLine("  http://" + currNode.Http.DnsHostname + ":" + currNode.Http.Port + "/default/default/hello.html?x-api-key=default");
            Console.WriteLine("  http://" + currNode.Http.DnsHostname + ":" + currNode.Http.Port + "/default/default/hello.html?x-api-key=default&metadata=true");
            Console.WriteLine("  http://" + currNode.Http.DnsHostname + ":" + currNode.Http.Port + "/default/default/hello.txt?x-api-key=default");
            Console.WriteLine("  http://" + currNode.Http.DnsHostname + ":" + currNode.Http.Port + "/default/default/hello.json?x-api-key=default"); 
            Console.WriteLine("");

            #endregion
        }

        private string SampleHtmlFile(string link)
        {
            string html =
                "<html>" + Environment.NewLine +
                "   <head>" + Environment.NewLine +
                "      <title>Welcome to Kvpbase!</title>" + Environment.NewLine +
                "      <style>" + Environment.NewLine +
                "          body {" + Environment.NewLine +
                "            font-family: arial;" + Environment.NewLine +
                "          }" + Environment.NewLine +
                "          h3 {" + Environment.NewLine +
                "            background-color: #e5e7ea;" + Environment.NewLine +
                "            color: #333333; " + Environment.NewLine +
                "            padding: 16px;" + Environment.NewLine +
                "            border: 16px;" + Environment.NewLine +
                "          }" + Environment.NewLine +
                "          p {" + Environment.NewLine +
                "            color: #333333; " + Environment.NewLine +
                "            padding: 4px;" + Environment.NewLine +
                "            border: 4px;" + Environment.NewLine +
                "          }" + Environment.NewLine +
                "          a {" + Environment.NewLine +
                "            background-color: #4cc468;" + Environment.NewLine +
                "            color: white;" + Environment.NewLine +
                "            padding: 4px;" + Environment.NewLine +
                "            border: 4px;" + Environment.NewLine +
                "         text-decoration: none; " + Environment.NewLine +
                "          }" + Environment.NewLine +
                "          li {" + Environment.NewLine +
                "            padding: 6px;" + Environment.NewLine +
                "            border: 6px;" + Environment.NewLine +
                "          }" + Environment.NewLine +
                "      </style>" + Environment.NewLine +
                "   </head>" + Environment.NewLine +
                "   <body>" + Environment.NewLine +
                "      <h3>Kvpbase Storage Server</h3>" + Environment.NewLine +
                "      <p>Congratulations, your Kvpbase Storage Server node is running!</p>" + Environment.NewLine +
                "      <p>" + Environment.NewLine + 
                "        <a href='" + link + "' target='_blank'>SDKs and Source Code</a>" + Environment.NewLine +
                "      </p>" + Environment.NewLine +
                "   </body>" + Environment.NewLine +
                "</html>";

            return html;
        }

        private string SampleJsonFile(string link)
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            ret.Add("Title", "Welcome to Kvpbase");
            ret.Add("Body", "If you can see this file, your Kvpbase node is running!");
            ret.Add("Github", link);
            return Common.SerializeJson(ret, true);
        }

        private string SampleTextFile(string link)
        {
            string text =
                "Welcome to Kvpbase!" + Environment.NewLine + Environment.NewLine +
                "If you can see this file, your Kvpbase node is running!  Now try " +
                "accessing this same URL in your browser, but use the .html extension!" + Environment.NewLine + Environment.NewLine +
                "Find us on Github here: " + link + Environment.NewLine + Environment.NewLine;

            return text;
        }

        #endregion
    }
}