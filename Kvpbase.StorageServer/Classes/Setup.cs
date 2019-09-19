using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

using DatabaseWrapper;
using SyslogLogging;

using Kvpbase.Classes;
using Kvpbase.Classes.Managers;
using Kvpbase.Containers;

namespace Kvpbase.Classes
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
            Settings settings = new Settings(); 
             
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
            //                          1         2         3         4         5         6         7
            //                 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("Thank you for using Kvpbase!  We'll put together a basic system configuration");
            Console.WriteLine("so you can be up and running quickly.  You'll want to modify the System.json");
            Console.WriteLine("file after to ensure a more secure operating environment."); 

            #endregion

            #region Initial-Settings
              
            settings.EnableConsole = true;
              
            settings.Server = new Settings.SettingsServer();
            settings.Server.Port = 8000;
            settings.Server.DnsHostname = "localhost";
            settings.Server.Ssl = false;
            settings.Server.HeaderApiKey = "x-api-key";
            settings.Server.HeaderEmail = "x-email";
            settings.Server.HeaderPassword = "x-password";
            settings.Server.MaxObjectSize = 2199023255552;      // 2TB object size
            settings.Server.MaxTransferSize = 536870912;        // 512MB transfer size
             
            settings.Storage = new Settings.SettingsStorage(); 
            settings.Storage.Directory = "./Storage/";
            Directory.CreateDirectory(settings.Storage.Directory);

            settings.Syslog = new Settings.SettingsSyslog();
            settings.Syslog.ConsoleLogging = true;
            settings.Syslog.Header = "kvpbase";
            settings.Syslog.ServerIp = "127.0.0.1";
            settings.Syslog.ServerPort = 514;
            settings.Syslog.LogHttpRequests = false;
            settings.Syslog.LogHttpResponses = false;
            settings.Syslog.MinimumLevel = 1;

            #endregion

            #region Databases

            //                          1         2         3         4         5         6         7
            //                 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("");
            Console.WriteLine("Kvpbase requires access to an external database, either Microsoft SQL Server,");
            Console.WriteLine("MySQL, or PostgreSQL, for holding configuration-related information.  Please");
            Console.WriteLine("provide access details for your database.  The user account supplied must have");
            Console.WriteLine("the ability to CREATE and DROP tables along with issue queries containing");
            Console.WriteLine("SELECT, INSERT, UPDATE, and DELETE.");
            Console.WriteLine("");
            Console.WriteLine("IMPORTANT: The database must be created before continuing.  Kvpbase will");
            Console.WriteLine("automatically create the tables for you.");
            Console.WriteLine("");

            settings.ConfigDatabase = new Settings.SettingsDatabase();

            bool dbSet = false;
            while (!dbSet)
            {
                string userInput = Common.InputString("Database type [mssql|mysql|pgsql]:", "mssql", false);
                switch (userInput)
                {
                    case "mssql":
                        settings.ConfigDatabase.Type = DatabaseWrapper.DbTypes.MsSql;
                        settings.ConfigDatabase.Hostname = Common.InputString("Hostname:", "localhost", false);
                        settings.ConfigDatabase.Port = Common.InputInteger("Port:", 1433, true, false);
                        settings.ConfigDatabase.Username = Common.InputString("Username:", "sa", false);
                        settings.ConfigDatabase.Password = Common.InputString("Password:", null, false);
                        settings.ConfigDatabase.InstanceName = Common.InputString("Instance (for SQLEXPRESS):", null, true);
                        settings.ConfigDatabase.DatabaseName = Common.InputString("Database name:", "kvpbaseconfig", false);
                        dbSet = true;
                        break;
                    case "mysql":
                        settings.ConfigDatabase.Type = DatabaseWrapper.DbTypes.MySql;
                        settings.ConfigDatabase.Hostname = Common.InputString("Hostname:", "localhost", false);
                        settings.ConfigDatabase.Port = Common.InputInteger("Port:", 3306, true, false);
                        settings.ConfigDatabase.Username = Common.InputString("Username:", "root", false);
                        settings.ConfigDatabase.Password = Common.InputString("Password:", null, false);
                        settings.ConfigDatabase.DatabaseName = Common.InputString("Schema name:", "kvpbaseconfig", false);
                        dbSet = true;
                        break;
                    case "pgsql":
                        settings.ConfigDatabase.Type = DatabaseWrapper.DbTypes.PgSql;
                        settings.ConfigDatabase.Hostname = Common.InputString("Hostname:", "localhost", false);
                        settings.ConfigDatabase.Port = Common.InputInteger("Port:", 5432, true, false);
                        settings.ConfigDatabase.Username = Common.InputString("Username:", "postgres", false);
                        settings.ConfigDatabase.Password = Common.InputString("Password:", null, false);
                        settings.ConfigDatabase.DatabaseName = Common.InputString("Schema name:", "kvpbaseconfig", false);
                        dbSet = true;
                        break;
                }
            }

            //                          1         2         3         4         5         6         7
            //                 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("");
            Console.WriteLine("Kvpbase also requires database access for storage metadata.  This database can");
            Console.WriteLine("be the same as your configuration database or different.");
            Console.WriteLine("");

            bool useConfigDatabase = Common.InputBoolean("Use configuration database settings for storage database?", true);

            if (useConfigDatabase)
            {
                settings.StorageDatabase = new Settings.SettingsDatabase();
                settings.StorageDatabase.Type = settings.ConfigDatabase.Type;
                settings.StorageDatabase.Hostname = settings.ConfigDatabase.Hostname;
                settings.StorageDatabase.Port = settings.ConfigDatabase.Port;
                settings.StorageDatabase.Username = settings.ConfigDatabase.Username;
                settings.StorageDatabase.Password = settings.ConfigDatabase.Password;
                settings.StorageDatabase.InstanceName = settings.ConfigDatabase.InstanceName;
                settings.StorageDatabase.DatabaseName = Common.InputString("Database name: ", "kvpbasedata", false);
            }
            else
            {
                dbSet = false;
                while (!dbSet)
                {
                    string userInput = Common.InputString("Database type [mssql|mysql|pgsql]:", "mssql", false);
                    switch (userInput)
                    {
                        case "mssql":
                            settings.StorageDatabase.Type = DatabaseWrapper.DbTypes.MsSql;
                            settings.StorageDatabase.Hostname = Common.InputString("Hostname:", "localhost", false);
                            settings.StorageDatabase.Port = Common.InputInteger("Port:", 1433, true, false);
                            settings.StorageDatabase.Username = Common.InputString("Username:", "sa", false);
                            settings.StorageDatabase.Password = Common.InputString("Password:", null, false);
                            settings.StorageDatabase.InstanceName = Common.InputString("Instance (for SQLEXPRESS):", null, true);
                            settings.StorageDatabase.DatabaseName = Common.InputString("Database name:", "kvpbaseconfig", false);
                            dbSet = true;
                            break;
                        case "mysql":
                            settings.StorageDatabase.Type = DatabaseWrapper.DbTypes.MySql;
                            settings.StorageDatabase.Hostname = Common.InputString("Hostname:", "localhost", false);
                            settings.StorageDatabase.Port = Common.InputInteger("Port:", 3306, true, false);
                            settings.StorageDatabase.Username = Common.InputString("Username:", "root", false);
                            settings.StorageDatabase.Password = Common.InputString("Password:", null, false);
                            settings.StorageDatabase.DatabaseName = Common.InputString("Schema name:", "kvpbaseconfig", false);
                            dbSet = true;
                            break;
                        case "pgsql":
                            settings.StorageDatabase.Type = DatabaseWrapper.DbTypes.PgSql;
                            settings.StorageDatabase.Hostname = Common.InputString("Hostname:", "localhost", false);
                            settings.StorageDatabase.Port = Common.InputInteger("Port:", 5432, true, false);
                            settings.StorageDatabase.Username = Common.InputString("Username:", "postgres", false);
                            settings.StorageDatabase.Password = Common.InputString("Password:", null, false);
                            settings.StorageDatabase.DatabaseName = Common.InputString("Schema name:", "kvpbaseconfig", false);
                            dbSet = true;
                            break;
                    }
                }
            }

            #endregion

            #region Write-Files-and-Records

            Console.WriteLine("");
            Console.WriteLine("| Writing system.json");

            Common.WriteFile("System.json", Encoding.UTF8.GetBytes(Common.SerializeJson(settings, true)));

            Console.WriteLine("| Initializing logging");

            LoggingModule logging = new LoggingModule("127.0.0.1", 514);

            Console.WriteLine(
                "| Initializing config DB: " + settings.ConfigDatabase.Hostname +
                "/" + settings.ConfigDatabase.DatabaseName +
                " [" + settings.ConfigDatabase.Type.ToString() + "]");

            DatabaseClient configDb = new DatabaseClient(
                settings.ConfigDatabase.Type,
                settings.ConfigDatabase.Hostname,
                settings.ConfigDatabase.Port,
                settings.ConfigDatabase.Username,
                settings.ConfigDatabase.Password,
                settings.ConfigDatabase.InstanceName,
                settings.ConfigDatabase.DatabaseName);
             
            Console.WriteLine(
                "| Initializing storage DB: " + settings.StorageDatabase.Hostname +
                "/" + settings.StorageDatabase.DatabaseName +
                " [" + settings.StorageDatabase.Type.ToString() + "]");

            DatabaseClient storageDb = new DatabaseClient(
                settings.StorageDatabase.Type,
                settings.StorageDatabase.Hostname,
                settings.StorageDatabase.Port,
                settings.StorageDatabase.Username,
                settings.StorageDatabase.Password,
                settings.StorageDatabase.InstanceName,
                settings.StorageDatabase.DatabaseName);
             
            Console.WriteLine("| Initializing configuration manager");

            ConfigManager configMgr = new ConfigManager(settings, logging, configDb);

            Console.WriteLine("| Adding user [default]");

            UserMaster currUser = new UserMaster();
            currUser.GUID = "default";
            currUser.Email = "default@default.com";
            currUser.Password = "default";
            currUser.Address1 = "123 Some Street";
            currUser.Cellphone = "408-555-1212";
            currUser.City = "San Jose";
            currUser.CompanyName = "Default Company";
            currUser.Country = "USA";
            currUser.FirstName = "First";
            currUser.LastName = "Last";
            currUser.PostalCode = "95128";
            currUser.State = "CA"; 
            currUser.CreatedUtc = timestamp;
            currUser.Active = true;

            configMgr.AddUser(currUser);
            currUser = configMgr.GetUserByGuid("default");

            Console.WriteLine("| Adding API key [default]");

            ApiKey currApiKey = new ApiKey();
            currApiKey = new ApiKey(); 
            currApiKey.GUID = "default";
            currApiKey.Notes = "Created by setup script";
            currApiKey.UserMasterId = currUser.Id;
            currApiKey.Active = true;
            currApiKey.CreatedUtc = timestamp;

            configMgr.AddApiKey(currApiKey);
            currApiKey = configMgr.GetApiKeyByGuid("default");

            Console.WriteLine("| Adding permission [default]");

            Permission currPerm = new Permission();
            currPerm.GUID = "default";
            currPerm.DeleteContainer = true;
            currPerm.DeleteObject = true;
            currPerm.ReadContainer = true;
            currPerm.ReadObject = true; 
            currPerm.WriteContainer = true;
            currPerm.WriteObject = true;
            currPerm.ApiKeyId = currApiKey.Id; 
            currPerm.Notes = "Created by setup script";
            currPerm.UserMasterId = currUser.Id;
            currPerm.Active = true;
            currPerm.CreatedUtc = timestamp;

            configMgr.AddPermission(currPerm);
            currPerm = configMgr.GetPermissionByGuid("default");

            Console.WriteLine("| Creating container [default]");

            string htmlFile = SampleHtmlFile("http://github.com/kvpbase");
            string textFile = SampleTextFile("http://github.com/kvpbase");
            string jsonFile = SampleJsonFile("http://github.com/kvpbase");

            ContainerManager containerMgr = new ContainerManager(settings, logging, configMgr, storageDb);

            Container container = new Container();
            container.UserGuid = "default";
            container.Name = "default";
            container.GUID = "default";
            container.ObjectsDirectory = settings.Storage.Directory + container.UserGuid + "/" + container.Name + "/";
            container.EnableAuditLogging = true;
            container.IsPublicRead = true;
            container.IsPublicWrite = false;

            containerMgr.Add(container); 

            ContainerClient client = null;
            containerMgr.GetContainerClient("default", "default", out client);

            Console.WriteLine("| Writing sample files to container [default]");

            ErrorCode error;
            client.WriteObject("hello.html", "text/html", Encoding.UTF8.GetBytes(htmlFile), null, out error);
            client.WriteObject("hello.txt", "text/plain", Encoding.UTF8.GetBytes(textFile), null, out error);
            client.WriteObject("hello.json", "application/json", Encoding.UTF8.GetBytes(jsonFile), null, out error);

            #endregion

            #region Wrap-Up

            //                         1         2         3         4         5         6         7
            //                12345678901234567890123456789012345678901234567890123456789012345678901234567890 
            Console.WriteLine("");
            Console.WriteLine(Common.Line(79, "-")); 
            Console.WriteLine("");
            Console.WriteLine("We have created your first user account and permissions.");
            Console.WriteLine("  Email    : " + currUser.Email);
            Console.WriteLine("  Password : " + currUser.Password);
            Console.WriteLine("  GUID     : " + currUser.GUID);
            Console.WriteLine("  API Key  : " + currApiKey.GUID); 
            Console.WriteLine("");
            Console.WriteLine("We've also created sample files for you so that you can see your node in");
            Console.WriteLine("action.  Go to the following URLs in your browser and see what happens!");
            Console.WriteLine("");
            Console.WriteLine("  http://localhost:8000/");
            Console.WriteLine("  http://localhost:8000/default/default?_container&_html");
            Console.WriteLine("  http://localhost:8000/default/default/hello.html");
            Console.WriteLine("  http://localhost:8000/default/default/hello.html?_metadata=true");
            Console.WriteLine("  http://localhost:8000/default/default/hello.txt");
            Console.WriteLine("  http://localhost:8000/default/default/hello.json"); 
            Console.WriteLine("");
            Console.WriteLine("Kvpbase is set to listen on 'localhost' and will only respond to requests");
            Console.WriteLine("from the local machine.  SSL is disabled.  Modify the System.json file to");
            Console.WriteLine("change these settings.");
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