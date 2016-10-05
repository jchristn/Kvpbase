using System;
using System.Collections.Generic;
using System.Text;
using SyslogLogging;

namespace Kvpbase
{
    public class Setup
    {
        public Setup()
        {
            RunSetup();
        }

        private void RunSetup()
        {
            #region Variables

            DateTime timestamp = DateTime.Now;
            Settings currSettings = new Settings();
            string separator = "";

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
            Console.WriteLine("kvpbase storage server");
            Console.WriteLine("");
            //          1         2         3         4         5         6         7
            // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("Thank you for using kvpbase!  We'll put together a basic system configuration");
            Console.WriteLine("so you can be up and running quickly.  You'll want to modify the system.json");
            Console.WriteLine("file after to ensure a more secure operating environment.");
            Console.WriteLine("");
            Console.WriteLine("Press ENTER to get started.");
            Console.WriteLine("");
            Console.WriteLine(Common.Line(79, "-"));
            Console.ReadLine();

            #endregion

            #region Initial-Settings

            currSettings.ProductName = "kvpbase";
            currSettings.ProductVersion = "2.0.1";
            currSettings.DocumentationUrl = "http://www.kvpbase.com/docs/";
            currSettings.LogoUrl = "http://kvpbase.com/Content/Images/cloud-only_25px.png";
            currSettings.HomepageUrl = "http://www.kvpbase.com";
            currSettings.SupportEmail = "support@maraudersoftware.com";

            int platform = (int)Environment.OSVersion.Platform;
            if ((platform == 4) || (platform == 6) || (platform == 128))
            {
                currSettings.Environment = "linux";
            }
            else
            {
                currSettings.Environment = "windows";
            }

            currSettings.EnableConsole = 1;

            #endregion

            #region Set-Defaults-for-Config-Sections

            separator = Common.GetPathSeparator(currSettings.Environment);

            #region Files

            currSettings.Files = new Settings.SettingsFiles();
            currSettings.Files.ApiKey = "." + separator + "api_key.config";
            currSettings.Files.Permission = "." + separator + "ApiKeyPermission.config";
            currSettings.Files.Topology = "." + separator + "topology.config";
            currSettings.Files.UserMaster = "." + separator + "user_master.config";

            #endregion

            #region Server

            currSettings.Server = new Settings.SettingsServer();
            currSettings.Server.HeaderApiKey = "x-api-key";
            currSettings.Server.HeaderEmail = "x-email";
            currSettings.Server.HeaderPassword = "x-password";
            currSettings.Server.HeaderToken = "x-token";
            currSettings.Server.HeaderVersion = "x-version";
            currSettings.Server.AdminApiKey = "kvpbaseadmin";
            currSettings.Server.TokenExpirationSec = 86400;
            currSettings.Server.FailedRequestsIntervalSec = 60;

            #endregion

            #region Redirection

            currSettings.Redirection = new Settings.SettingsRedirection();
            currSettings.Redirection.DeleteRedirectHttpStatus = 301;
            currSettings.Redirection.DeleteRedirectString = "Moved Permanently";
            currSettings.Redirection.DeleteRedirectionMode = "proxy";
            currSettings.Redirection.ReadRedirectHttpStatus = 301;
            currSettings.Redirection.ReadRedirectString = "Moved Permanently";
            currSettings.Redirection.ReadRedirectionMode = "proxy";
            currSettings.Redirection.WriteRedirectHttpStatus = 301;
            currSettings.Redirection.WriteRedirectString = "Moved Permanently";
            currSettings.Redirection.WriteRedirectionMode = "proxy";
            currSettings.Redirection.SearchRedirectHttpStatus = 301;
            currSettings.Redirection.SearchRedirectString = "Moved Permanently";
            currSettings.Redirection.SearchRedirectionMode = "proxy";

            #endregion

            #region Topology

            currSettings.Topology = new Settings.SettingsTopology();
            currSettings.Topology.RefreshSec = 10;

            #endregion

            #region Perfmon

            currSettings.Perfmon = new Settings.SettingsPerfmon();
            currSettings.Perfmon.Enable = 1;
            currSettings.Perfmon.RefreshSec = 10;
            currSettings.Perfmon.Syslog = 0;

            #endregion

            #region Storage

            currSettings.Storage = new Settings.SettingsStorage();
            currSettings.Storage.Directory = "." + separator + "storage" + separator;
            currSettings.Storage.MaxObjectSize = 512000000;
            Console.WriteLine("");
            //          1         2         3         4         5         6         7
            // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("Is this a gateway-mode kvpbase node (true)?  Gateway-mode is used when the node");
            Console.WriteLine("uses storage directories that live on other systems or contain data that can");
            Console.WriteLine("be accessed through means other than kvpbase, such as a file share.  Enabling");
            Console.WriteLine("gateway mode allows accessing the data through means outside of kvpbase by");
            Console.WriteLine("disabling encryption and compression.");
            Console.WriteLine("");
            bool is_gateway_mode = Common.InputBoolean("Configure this node for gateway-mode", true);
            Console.WriteLine("");

            if (is_gateway_mode)
            {
                currSettings.Storage.GatewayMode = 1;
                currSettings.Storage.DefaultCompress = 0;
                currSettings.Storage.DefaultEncrypt = 0;
            }
            else
            {
                currSettings.Storage.GatewayMode = 0;
                currSettings.Storage.DefaultCompress = 1;
                currSettings.Storage.DefaultEncrypt = 1;
            }

            #endregion

            #region Messages

            currSettings.Messages = new Settings.SettingsMessages();
            currSettings.Messages.Directory = "." + separator + "messages" + separator;
            currSettings.Messages.RefreshSec = 10;

            #endregion

            #region Expiration

            currSettings.Expiration = new Settings.SettingsExpiration();
            currSettings.Expiration.Directory = "." + separator + "expiration" + separator;
            currSettings.Expiration.RefreshSec = 10;
            currSettings.Expiration.DefaultExpirationSec = 0;

            #endregion

            #region Replication

            currSettings.Replication = new Settings.SettingsReplication();
            currSettings.Replication.Directory = "." + separator + "replication" + separator;
            currSettings.Replication.RefreshSec = 10;
            currSettings.Replication.ReplicationMode = "sync";

            #endregion

            #region Bunker

            currSettings.Bunker = new Settings.SettingsBunker();
            currSettings.Bunker.Directory = "." + separator + "bunker" + separator;
            currSettings.Bunker.Enable = 0;
            currSettings.Bunker.RefreshSec = 30;
            currSettings.Bunker.Nodes = null;

            #endregion

            #region Pubfiles

            currSettings.PublicObj = new Settings.SettingsPublicObj();
            currSettings.PublicObj.Directory = "." + separator + "pubfiles" + separator;
            currSettings.PublicObj.RefreshSec = 600;
            currSettings.PublicObj.DefaultExpirationSec = 7776000;

            #endregion

            #region Tasks

            currSettings.Tasks = new Settings.SettingsTasks();
            currSettings.Tasks.Directory = "." + separator + "tasks" + separator;
            currSettings.Tasks.RefreshSec = 10;

            #endregion

            #region Logger

            currSettings.Logger = new Settings.SettingsLogger();
            currSettings.Logger.RefreshSec = 10;

            #endregion

            #region Syslog

            currSettings.Syslog = new Settings.SettingsSyslog();
            currSettings.Syslog.ConsoleLogging = 1;
            currSettings.Syslog.Header = "kvpbase";
            currSettings.Syslog.ServerIp = "127.0.0.1";
            currSettings.Syslog.ServerPort = 514;
            currSettings.Syslog.LogHttpRequests = 0;
            currSettings.Syslog.LogHttpResponses = 0;
            currSettings.Syslog.MinimumLevel = 1;

            #endregion

            #region Email

            currSettings.Email = new Settings.SettingsEmail();

            #endregion

            #region Encryption

            currSettings.Encryption = new Settings.SettingsEncryption();
            currSettings.Encryption.Mode = "local";
            currSettings.Encryption.Iv = "0000000000000000";
            currSettings.Encryption.Passphrase = "0000000000000000";
            currSettings.Encryption.Salt = "0000000000000000";

            #endregion

            #region REST

            currSettings.Rest = new Settings.SettingsRest();
            currSettings.Rest.AcceptInvalidCerts = 0;
            currSettings.Rest.UseWebProxy = 0;

            #endregion

            #region Mailgun

            currSettings.Mailgun = new Settings.SettingsMailgun();

            #endregion

            #endregion
            
            #region System-Config

            if (
                Common.FileExists("system.json")
                )
            {
                Console.WriteLine("System configuration file already exists.");
                if (Common.InputBoolean("Do you wish to overwrite this file", true))
                {
                    Common.DeleteFile("system.json");
                    if (!Common.WriteFile("system.json", Common.SerializeJson(currSettings), false))
                    {
                        Common.ExitApplication("setup", "Unable to write system.json", -1);
                        return;
                    }
                }
            }
            else
            {
                if (!Common.WriteFile("system.json", Common.SerializeJson(currSettings), false))
                {
                    Common.ExitApplication("setup", "Unable to write system.json", -1);
                    return;
                }
            }

            #endregion

            #region Users-API-Keys-and-Permissions

            if (
                Common.FileExists(currSettings.Files.ApiKey)
                || Common.FileExists(currSettings.Files.Permission)
                || Common.FileExists(currSettings.Files.UserMaster)
                )
            {
                Console.WriteLine("Configuration files already exist for API keys, users, and/or permissions.");
                if (Common.InputBoolean("Do you wish to overwrite these files", true))
                {
                    Common.DeleteFile(currSettings.Files.ApiKey);
                    Common.DeleteFile(currSettings.Files.Permission);
                    Common.DeleteFile(currSettings.Files.UserMaster);

                    Console.WriteLine("Creating new configuration files for API keys, users, and permissions.");

                    currApiKey = new ApiKey();
                    currApiKey.Active = 1;
                    currApiKey.ApiKeyId = 1;
                    currApiKey.Created = timestamp;
                    currApiKey.LastUpdate = timestamp;
                    currApiKey.Expiration = timestamp.AddYears(100);
                    currApiKey.Guid = "default";
                    currApiKey.Notes = "Created by setup script";
                    currApiKey.UserMasterId = 1;
                    apiKeys.Add(currApiKey);

                    currPerm = new ApiKeyPermission();
                    currPerm.Active = 1;
                    currPerm.AllowDeleteContainer = 1;
                    currPerm.AllowDeleteObject = 1;
                    currPerm.AllowReadContainer = 1;
                    currPerm.AllowReadObject = 1;
                    currPerm.AllowSearch = 1;
                    currPerm.AllowWriteContainer = 1;
                    currPerm.AllowWriteObject = 1;
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
                    currUser.IsAdmin = 1;
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

                    if (!Common.WriteFile(currSettings.Files.ApiKey, Common.SerializeJson(apiKeys), false))
                    {
                        Common.ExitApplication("setup", "Unable to write " + currSettings.Files.ApiKey, -1);
                        return;
                    }

                    if (!Common.WriteFile(currSettings.Files.Permission, Common.SerializeJson(permissions), false))
                    {
                        Common.ExitApplication("setup", "Unable to write " + currSettings.Files.Permission, -1);
                        return;
                    }

                    if (!Common.WriteFile(currSettings.Files.UserMaster, Common.SerializeJson(users), false))
                    {
                        Common.ExitApplication("setup", "Unable to write " + currSettings.Files.UserMaster, -1);
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
                }
                else
                {
                    Console.WriteLine("Existing files were left in tact.");
                }
            }
            else
            {
                currApiKey = new ApiKey();
                currApiKey.Active = 1;
                currApiKey.ApiKeyId = 1;
                currApiKey.Created = timestamp;
                currApiKey.LastUpdate = timestamp;
                currApiKey.Expiration = timestamp.AddYears(100);
                currApiKey.Guid = "default";
                currApiKey.Notes = "Created by setup script";
                currApiKey.UserMasterId = 1;
                apiKeys.Add(currApiKey);

                currPerm = new ApiKeyPermission();
                currPerm.Active = 1;
                currPerm.AllowDeleteContainer = 1;
                currPerm.AllowDeleteObject = 1;
                currPerm.AllowReadContainer = 1;
                currPerm.AllowReadObject = 1;
                currPerm.AllowSearch = 1;
                currPerm.AllowWriteContainer = 1;
                currPerm.AllowWriteObject = 1;
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
                currUser.IsAdmin = 1;
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

                if (!Common.WriteFile(currSettings.Files.ApiKey, Common.SerializeJson(apiKeys), false))
                {
                    Common.ExitApplication("setup", "Unable to write " + currSettings.Files.ApiKey, -1);
                    return;
                }

                if (!Common.WriteFile(currSettings.Files.Permission, Common.SerializeJson(permissions), false))
                {
                    Common.ExitApplication("setup", "Unable to write " + currSettings.Files.Permission, -1);
                    return;
                }

                if (!Common.WriteFile(currSettings.Files.UserMaster, Common.SerializeJson(users), false))
                {
                    Common.ExitApplication("setup", "Unable to write " + currSettings.Files.UserMaster, -1);
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
            }

            #endregion

            #region Topology

            if (Common.FileExists(currSettings.Files.Topology))
            {
                #region File-Exists

                Console.WriteLine("Configuration file already exists for topology.");
                if (Common.InputBoolean("Do you wish to overwrite this file", true))
                {
                    #region Overwrite

                    Common.DeleteFile(currSettings.Files.Topology);

                    currTopology = new Topology();
                    currTopology.CurrNodeId = 1;

                    currTopology.Nodes = new List<Node>();
                    currNode = new Node();
                    currNode.DnsHostname = "localhost";
                    currNode.Name = "localhost";
                    currNode.Neighbors = null;
                    currNode.NodeId = 1;
                    currNode.Port = 8080;
                    currNode.Ssl = 0;

                    switch (currSettings.Environment)
                    {
                        case "linux":
                            Console.WriteLine("");
                            //          1         2         3         4         5         6         7
                            // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
                            Console.WriteLine("IMPORTANT: for Linux and Mac environments, kvpbase can only receive requests on");
                            Console.WriteLine("one hostname.  The hostname you set here must either be the hostname in the URL");
                            Console.WriteLine("used by the requestor, or, set in the HOST header of each request.");
                            Console.WriteLine("");
                            Console.WriteLine("If you set the hostname to 'localhost', this node will ONLY receive and handle");
                            Console.WriteLine("requests destined to 'localhost', i.e. it will only handle local requests.");
                            Console.WriteLine("");

                            currNode.DnsHostname = Common.InputString("On which hostname shall this node listen?", "localhost", false);
                            break;

                        case "windows":
                            currNode.DnsHostname = "+";
                            break;
                    }

                    Console.WriteLine("");
                    currNode.Port = Common.InputInteger("On which port should this node listen?", 8080, true, false);
                    Console.WriteLine("");

                    currTopology.Nodes = new List<Node>();
                    currTopology.Nodes.Add(currNode);
                    currTopology.Replicas = null;

                    if (!Common.WriteFile(currSettings.Files.Topology, Common.SerializeJson(currTopology), false))
                    {
                        Common.ExitApplication("setup", "Unable to write " + currSettings.Files.Topology, -1);
                        return;
                    }
                    //          1         2         3         4         5         6         7
                    // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
                    Console.WriteLine("We've created your topology file.  This node is configured to");
                    Console.WriteLine("use HTTP (not HTTPS) and is accessible at the following URL:");
                    Console.WriteLine("");
                    Console.WriteLine("  http://" + currNode.DnsHostname + ":" + currNode.Port);

                    if (String.Compare(currSettings.Environment, "windows") == 0)
                        Console.WriteLine("  Windows: '+' indicates accessibility on any IP or hostname");

                    Console.WriteLine("");
                    Console.WriteLine("Be sure to install an SSL certificate and modify your topology.config file to");
                    Console.WriteLine("use SSL to maximize security and set the correct hostname.");
                    Console.WriteLine("");

                    #endregion
                }

                #endregion
            }
            else
            {
                #region New-File

                currTopology = new Topology();
                currTopology.CurrNodeId = 1;

                currTopology.Nodes = new List<Node>();
                currNode = new Node();
                currNode.DnsHostname = "localhost";
                currNode.Name = "localhost";
                currNode.Neighbors = null;
                currNode.NodeId = 1;
                currNode.Port = 8080;
                currNode.Ssl = 0;

                switch (currSettings.Environment)
                {
                    case "linux":
                        Console.WriteLine("");
                        //          1         2         3         4         5         6         7
                        // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
                        Console.WriteLine("IMPORTANT: for Linux and Mac environments, kvpbase can only receive requests on");
                        Console.WriteLine("one hostname.  The hostname you set here must either be the hostname in the URL");
                        Console.WriteLine("used by the requestor, or, set in the HOST header of each request.");
                        Console.WriteLine("");
                        Console.WriteLine("If you set the hostname to 'localhost', this node will ONLY receive and handle");
                        Console.WriteLine("requests destined to 'localhost', i.e. it will only handle local requests.");
                        Console.WriteLine("");

                        currNode.DnsHostname = Common.InputString("On which hostname shall this node listen?", "localhost", false);
                        break;

                    case "windows":
                        currNode.DnsHostname = "+";
                        break;
                }

                Console.WriteLine("");
                currNode.Port = Common.InputInteger("On which port should this node listen?", 8080, true, false);
                Console.WriteLine("");

                currTopology.Nodes = new List<Node>();
                currTopology.Nodes.Add(currNode);
                currTopology.Replicas = null;

                if (!Common.WriteFile(currSettings.Files.Topology, Common.SerializeJson(currTopology), false))
                {
                    Common.ExitApplication("setup", "Unable to write " + currSettings.Files.Topology, -1);
                    return;
                }

                //          1         2         3         4         5         6         7
                // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
                Console.WriteLine("We've created your topology file.  This node is configured to");
                Console.WriteLine("use HTTP (not HTTPS) and is accessible at the following URL:");
                Console.WriteLine("");
                Console.WriteLine("  http://" + currNode.DnsHostname + ":" + currNode.Port);

                if (String.Compare(currSettings.Environment, "windows") == 0)
                    Console.WriteLine("  Windows: '+' indicates accessibility on any IP or hostname");

                Console.WriteLine("");
                Console.WriteLine("Be sure to install an SSL certificate and modify your topology.config file to");
                Console.WriteLine("use SSL to maximize security and set the correct hostname.");
                Console.WriteLine("");

                #endregion
            }
            
            #endregion

            #region Create-Directories

            currSettings.Storage.Directory = "." + separator + "storage" + separator;
            currSettings.Messages.Directory = "." + separator + "messages" + separator;
            currSettings.Expiration.Directory = "." + separator + "expiration" + separator;
            currSettings.Replication.Directory = "." + separator + "replication" + separator;
            currSettings.Bunker.Directory = "." + separator + "bunker" + separator;
            currSettings.PublicObj.Directory = "." + separator + "pubfiles" + separator;
            currSettings.Tasks.Directory = "." + separator + "tasks" + separator;

            if (!Common.CreateDirectory(currSettings.Storage.Directory))
            {
                Common.ExitApplication("setup", "Unable to create directory " + currSettings.Storage.Directory, -1);
                return;
            }

            if (!Common.CreateDirectory(currSettings.Messages.Directory))
            {
                Common.ExitApplication("setup", "Unable to create directory " + currSettings.Messages.Directory, -1);
                return;
            }

            if (!Common.CreateDirectory(currSettings.Expiration.Directory))
            {
                Common.ExitApplication("setup", "Unable to create directory " + currSettings.Expiration.Directory, -1);
                return;
            }

            if (!Common.CreateDirectory(currSettings.Replication.Directory))
            {
                Common.ExitApplication("setup", "Unable to create directory " + currSettings.Replication.Directory, -1);
                return;
            }

            if (!Common.CreateDirectory(currSettings.Bunker.Directory))
            {
                Common.ExitApplication("setup", "Unable to create directory " + currSettings.Bunker.Directory, -1);
                return;
            }

            if (!Common.CreateDirectory(currSettings.PublicObj.Directory))
            {
                Common.ExitApplication("setup", "Unable to create directory " + currSettings.PublicObj.Directory, -1);
                return;
            }

            if (!Common.CreateDirectory(currSettings.Tasks.Directory))
            {
                Common.ExitApplication("setup", "Unable to create directory " + currSettings.Tasks.Directory, -1);
                return;
            }

            if (!Common.CreateDirectory("." + separator + "actions" + separator))
            {
                Common.ExitApplication("setup", "Unable to create directory actions subdirectory", -1);
                return;
            }

            #endregion

            #region Create-Sample-Objects

            string html_file = SampleHtmlFile(currSettings.DocumentationUrl, "http://www.kvpbase.com/support", "http://github.com/kvpbase");
            string text_file = SampleTextFile(currSettings.DocumentationUrl, "http://www.kvpbase.com/support", "http://github.com/kvpbase");
            string json_file = SampleJsonFile(currSettings.DocumentationUrl, "http://www.kvpbase.com/support", "http://github.com/kvpbase");

            if (!Common.CreateDirectory(currSettings.Storage.Directory + "default"))
            {
                Common.ExitApplication("setup", "Unable to create directory " + currSettings.Storage.Directory + "default", -1);
                return;
            }

            Obj htmlObj = new Obj();
            htmlObj.IsCompressed = 0;
            htmlObj.IsContainer = 0;
            htmlObj.ContainerPath = null;
            htmlObj.ContentType = "text/html";
            htmlObj.Created = DateTime.Now;
            htmlObj.IsEncrypted = 0;
            htmlObj.Key = "hello.html";
            htmlObj.LastAccess = DateTime.Now;
            htmlObj.LastUpdate = DateTime.Now;
            htmlObj.PrimaryNode = currNode;
            htmlObj.PrimaryUrlWithQs = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/default/hello.html?x-api-key=default";
            htmlObj.PrimaryUrlWithoutQs = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/default/hello.html";
            htmlObj.Replicas = null;
            htmlObj.ReplicationMode = "none";
            htmlObj.Tags = null;
            htmlObj.UserGuid = "default";
            htmlObj.Value = Encoding.UTF8.GetBytes(html_file);
            htmlObj.Md5Hash = Common.Md5(htmlObj.Value);
            htmlObj.DiskPath = currSettings.Storage.Directory + "default" + separator + "hello.html";

            Obj text_obj = new Obj();
            text_obj.IsCompressed = 0;
            text_obj.IsContainer = 0;
            text_obj.ContainerPath = null;
            text_obj.ContentType = "text/plain";
            text_obj.Created = DateTime.Now;
            text_obj.IsEncrypted = 0;
            text_obj.Key = "hello.txt";
            text_obj.LastAccess = DateTime.Now;
            text_obj.LastUpdate = DateTime.Now;
            text_obj.PrimaryNode = currNode;
            text_obj.PrimaryUrlWithQs = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/default/hello.txt?x-api-key=default";
            text_obj.PrimaryUrlWithoutQs = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/default/hello.txt";
            text_obj.Replicas = null;
            text_obj.ReplicationMode = "none";
            text_obj.Tags = null;
            text_obj.UserGuid = "default";
            text_obj.Value = Encoding.UTF8.GetBytes(text_file);
            text_obj.Md5Hash = Common.Md5(text_obj.Value);
            text_obj.DiskPath = currSettings.Storage.Directory + "default" + separator + "hello.txt";

            Obj json_obj = new Obj();
            json_obj.IsCompressed = 0;
            json_obj.IsContainer = 0;
            json_obj.ContainerPath = null;
            json_obj.ContentType = "application/json";
            json_obj.Created = DateTime.Now;
            json_obj.IsEncrypted = 0;
            json_obj.Key = "hello.json";
            json_obj.LastAccess = DateTime.Now;
            json_obj.LastUpdate = DateTime.Now;
            json_obj.PrimaryNode = currNode;
            json_obj.PrimaryUrlWithQs = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/default/hello.json?x-api-key=default";
            json_obj.PrimaryUrlWithoutQs = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/default/hello.json";
            json_obj.Replicas = null;
            json_obj.ReplicationMode = "none";
            json_obj.Tags = null;
            json_obj.UserGuid = "default";
            json_obj.Value = Encoding.UTF8.GetBytes(json_file);
            json_obj.Md5Hash = Common.Md5(json_obj.Value);
            json_obj.DiskPath = currSettings.Storage.Directory + "default" + separator + "hello.json";

            if (Common.IsTrue(currSettings.Storage.GatewayMode))
            {
                if (!Common.WriteFile(currSettings.Storage.Directory + "default" + separator + "hello.html", html_file, false))
                {
                    Common.ExitApplication("setup", "Unable to create sample file storage/default/hello.html", -1);
                    return;
                }

                if (!Common.WriteFile(currSettings.Storage.Directory + "default" + separator + "hello.txt", text_file, false))
                {
                    Common.ExitApplication("setup", "Unable to create sample file storage/default/hello.txt", -1);
                    return;
                }

                if (!Common.WriteFile(currSettings.Storage.Directory + "default" + separator + "hello.json", json_file, false))
                {
                    Common.ExitApplication("setup", "Unable to create sample file storage/default/hello.json", -1);
                    return;
                }
            }
            else
            {
                if (!Common.WriteFile(currSettings.Storage.Directory + "default" + separator + "hello.html", Common.SerializeJson(htmlObj), false))
                {
                    Common.ExitApplication("setup", "Unable to create sample file storage/default/hello.html", -1);
                    return;
                }

                if (!Common.WriteFile(currSettings.Storage.Directory + "default" + separator + "hello.txt", Common.SerializeJson(text_obj), false))
                {
                    Common.ExitApplication("setup", "Unable to create sample file storage/default/hello.txt", -1);
                    return;
                }

                if (!Common.WriteFile(currSettings.Storage.Directory + "default" + separator + "hello.json", Common.SerializeJson(json_obj), false))
                {
                    Common.ExitApplication("setup", "Unable to create sample file storage/default/hello.json", -1);
                    return;
                }
            }

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

            switch (currSettings.Environment)
            {
                case "linux":
                    Console.WriteLine("  http://" + currNode.DnsHostname + ":" + currNode.Port + "/default/hello.html?x-api-key=default");
                    Console.WriteLine("  http://" + currNode.DnsHostname + ":" + currNode.Port + "/default/hello.html?x-api-key=default&metadata=true");
                    Console.WriteLine("  http://" + currNode.DnsHostname + ":" + currNode.Port + "/default/hello.txt?x-api-key=default");
                    Console.WriteLine("  http://" + currNode.DnsHostname + ":" + currNode.Port + "/default/hello.json?x-api-key=default");
                    break;

                case "windows":
                    Console.WriteLine("  http://localhost:" + currNode.Port + "/default/hello.html?x-api-key=default");
                    Console.WriteLine("  http://localhost:" + currNode.Port + "/default/hello.html?x-api-key=default&metadata=true");
                    Console.WriteLine("  http://localhost:" + currNode.Port + "/default/hello.txt?x-api-key=default");
                    Console.WriteLine("  http://localhost:" + currNode.Port + "/default/hello.json?x-api-key=default");
                    break;
            }

            Console.WriteLine("");

            #endregion
        }

        private string SampleHtmlFile(string docLink, string supportLink, string sdkLink)
        {
            string html =
                "<html>" + Environment.NewLine +
                "   <head>" + Environment.NewLine +
                "      <title>Welcome to kvpbase!</title>" + Environment.NewLine +
                "      <style>" + Environment.NewLine +
                "      	body {" + Environment.NewLine +
                "  		  font-family: arial;" + Environment.NewLine +
                "      	}" + Environment.NewLine +
                "      	h3 {" + Environment.NewLine +
                "  		  background-color: grey;" + Environment.NewLine +
                "         color: white; " + Environment.NewLine +
                "  		  padding: 16px;" + Environment.NewLine +
                "  		  border: 16px;" + Environment.NewLine +
                "      	}" + Environment.NewLine +
                "      	p {" + Environment.NewLine +
                "      	  padding: 4px;" + Environment.NewLine +
                "      	  border: 4px;" + Environment.NewLine +
                "      	}" + Environment.NewLine +
                "      	a {" + Environment.NewLine +
                "      	  background-color: green;" + Environment.NewLine +
                "      	  color: white;" + Environment.NewLine +
                "      	  padding: 4px;" + Environment.NewLine +
                "      	  border: 4px;" + Environment.NewLine +
                "      	}" + Environment.NewLine +
                "      	li {" + Environment.NewLine +
                "      	  padding: 6px;" + Environment.NewLine +
                "      	  border: 6px;" + Environment.NewLine +
                "      	}" + Environment.NewLine +
                "      </style>" + Environment.NewLine +
                "   </head>" + Environment.NewLine +
                "   <body>" + Environment.NewLine +
                "      <h3>Welcome to kvpbase!</h3>" + Environment.NewLine +
                "      <p>If you can see this file, your kvpbase node is running!</p>" + Environment.NewLine +
                "      <p>If you opened this file using your browser, it should have been rendered as HTML. That's because kvpbase preserves the content-type when you write an object, meaning you can use your kvpbase nodes as an extension of your web servers!" + Environment.NewLine +
                "      </p>" + Environment.NewLine +
                "      <p>Remember these helpful links!</p>" + Environment.NewLine +
                "      	<ul>" + Environment.NewLine +
                "      	  <li><a href='" + docLink + "' target='_blank'>API Documentation</a></li>" + Environment.NewLine +
                "      	  <li><a href='" + supportLink + "' target='_blank'>Support Portal</a></li>" + Environment.NewLine +
                "      	  <li><a href='" + sdkLink + "' target='_blank'>Download SDKs</a></li>" + Environment.NewLine +
                "      	</ul>" + Environment.NewLine +
                "   </body>" + Environment.NewLine +
                "</html>";

            return html;
        }

        private string SampleJsonFile(string docLink, string supportLink, string sdkLink)
        {
            string json =
                "{" + Environment.NewLine +
                "  \"title\": \"Welcome to kvpbase!\", " + Environment.NewLine +
                "  \"data\": \"If you can see this file, your kvpbase node is running!\", " + Environment.NewLine +
                "  \"other_urls\": [" + Environment.NewLine +
                "    \"http://localhost:8080/default/hello.html?x-api-key=default\", " + Environment.NewLine +
                "    \"http://localhost:8080/default/hello.html?x-api-key=default&metadata=true\", " + Environment.NewLine +
                "    \"http://localhost:8080/default/hello.txt?x-api-key=default\" " + Environment.NewLine +
                "  ]," + Environment.NewLine +
                "  \"documentation\": \"" + docLink + "\"," + Environment.NewLine +
                "  \"support\": \"" + supportLink + "\"," + Environment.NewLine +
                "  \"sdks\": \"" + sdkLink + "\"" + Environment.NewLine +
                "}";

            return json;
        }

        private string SampleTextFile(string docLink, string supportLink, string sdkLink)
        {
            string text =
                "Welcome to kvpbase!" + Environment.NewLine + Environment.NewLine +
                "If you can see this file, your kvpbase node is running!  Now try " +
                "accessing this same URL in your browser, but use the .html extension!" + Environment.NewLine + Environment.NewLine +
                "Remember - documentation is available here: " + docLink + Environment.NewLine + Environment.NewLine +
                "And, our support portal is available here: " + supportLink + Environment.NewLine + Environment.NewLine +
                "Finally, download SDKs here: " + sdkLink + Environment.NewLine + Environment.NewLine;

            return text;
        }
    }
}