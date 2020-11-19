using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using SyslogLogging;
using Watson.ORM;
using Watson.ORM.Core;
using Kvpbase.StorageServer.Classes;
using Kvpbase.StorageServer.Classes.DatabaseObjects;
using Kvpbase.StorageServer.Classes.Managers;
using Common = Kvpbase.StorageServer.Classes.Common;

namespace Kvpbase.StorageServer.Classes
{
    internal class Setup
    {
        internal Setup()
        {
            RunSetup();
        }
        
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
            settings.Server.MaxObjectSize = 2199023255552;      // 2TB object size
            settings.Server.MaxTransferSize = 536870912;        // 512MB transfer size
             
            settings.Storage = new Settings.SettingsStorage(); 
            settings.Storage.Directory = "./storage/";
            settings.Storage.LockExpirationSeconds = 300;
            Directory.CreateDirectory(settings.Storage.Directory);

            settings.Syslog = new Settings.SettingsSyslog();
            settings.Syslog.ConsoleLogging = true;
            settings.Syslog.Header = "kvpbase";
            settings.Syslog.ServerIp = "127.0.0.1";
            settings.Syslog.ServerPort = 514;
            settings.Syslog.MinimumLevel = Severity.Info;
            settings.Syslog.FileLogging = true;
            settings.Syslog.LogDirectory = "./logs/";
            
            if (!Directory.Exists(settings.Syslog.LogDirectory)) Directory.CreateDirectory(settings.Syslog.LogDirectory);

            settings.Debug = new Settings.SettingsDebug();
            settings.Debug.Database = false;
            settings.Debug.HttpRequest = false; 

            #endregion

            #region Databases

            //                          1         2         3         4         5         6         7
            //                 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("");
            Console.WriteLine("Kvpbase requires access to a database, either Sqlite, Microsoft SQL Server,");
            Console.WriteLine("MySQL, or PostgreSQL.  Please provide access details for your database.  The");
            Console.WriteLine("user account supplied must have the ability to CREATE and DROP tables along");
            Console.WriteLine("with issue queries containing SELECT, INSERT, UPDATE, and DELETE.  Setup will");
            Console.WriteLine("attempt to create tables on your behalf if they dont exist.");
            Console.WriteLine("");

            bool dbSet = false;
            while (!dbSet)
            {
                string userInput = Common.InputString("Database type [sqlite|sqlserver|mysql|postgresql]:", "sqlite", false);
                switch (userInput)
                {
                    case "sqlite":
                        settings.Database = new DatabaseSettings(
                            Common.InputString("Filename:", "./kvpbase.db", false)
                            );

                        //                          1         2         3         4         5         6         7
                        //                 12345678901234567890123456789012345678901234567890123456789012345678901234567890
                        Console.WriteLine("");
                        Console.WriteLine("IMPORTANT: Using Sqlite in production is not recommended if deploying within a");
                        Console.WriteLine("containerized environment and the database file is stored within the container.");
                        Console.WriteLine("Store the database file in external storage to ensure persistence.");
                        Console.WriteLine("");
                        dbSet = true;
                        break;

                    case "sqlserver":
                        settings.Database = new DatabaseSettings(
                            Common.InputString("Hostname:", "localhost", false),
                            Common.InputInteger("Port:", 1433, true, false),
                            Common.InputString("Username:", "sa", false),
                            Common.InputString("Password:", null, false),
                            Common.InputString("Instance (for SQLEXPRESS):", null, true),
                            Common.InputString("Database name:", "kvpbase", false)
                            );
                        dbSet = true;
                        break;
                    case "mysql":
                        settings.Database = new DatabaseSettings(
                            DbTypes.Mysql,
                            Common.InputString("Hostname:", "localhost", false),
                            Common.InputInteger("Port:", 3306, true, false),
                            Common.InputString("Username:", "root", false),
                            Common.InputString("Password:", null, false),
                            Common.InputString("Schema name:", "kvpbase", false)
                            );
                        dbSet = true;
                        break;
                    case "postgresql":
                        settings.Database = new DatabaseSettings(
                            DbTypes.Postgresql,
                            Common.InputString("Hostname:", "localhost", false),
                            Common.InputInteger("Port:", 5432, true, false),
                            Common.InputString("Username:", "postgres", false),
                            Common.InputString("Password:", null, false),
                            Common.InputString("Schema name:", "kvpbase", false)
                            );
                        dbSet = true;
                        break;
                }
            }

            #endregion

            #region Write-Files-and-Records
             
            Console.WriteLine("| Writing system.json");

            Common.WriteFile("./system.json", Encoding.UTF8.GetBytes(Common.SerializeJson(settings, true)));

            Console.WriteLine("| Initializing logging");

            LoggingModule logging = new LoggingModule("127.0.0.1", 514);
            logging.MinimumSeverity = Severity.Info;

            Console.WriteLine("| Initializing database");

            WatsonORM orm = new WatsonORM(settings.Database);
            orm.InitializeDatabase();
            orm.InitializeTable(typeof(ApiKey));
            orm.InitializeTable(typeof(AuditLogEntry));
            orm.InitializeTable(typeof(Container));
            orm.InitializeTable(typeof(ContainerKeyValuePair));
            orm.InitializeTable(typeof(ObjectKeyValuePair));
            orm.InitializeTable(typeof(ObjectMetadata));
            orm.InitializeTable(typeof(Permission));
            orm.InitializeTable(typeof(UrlLock));
            orm.InitializeTable(typeof(UserMaster)); 
             
            Console.WriteLine("| Adding user [default]");

            UserMaster user = new UserMaster();
            user.GUID = "default";
            user.Email = "default@default.com";
            user.Password = "default";
            user.FirstName = "Default";
            user.LastName = "User";
            user.CreatedUtc = timestamp;
            user.Active = true; 
            user = orm.Insert<UserMaster>(user);

            Console.WriteLine("| Adding API key [default]");

            ApiKey apiKey = new ApiKey();
            apiKey = new ApiKey(); 
            apiKey.GUID = "default"; 
            apiKey.UserGUID = user.GUID;
            apiKey.Active = true;  
            apiKey = orm.Insert<ApiKey>(apiKey); 

            Console.WriteLine("| Adding permission [default]");

            Permission perm = new Permission();
            perm.GUID = Guid.NewGuid().ToString();
            perm.UserGUID = user.GUID;
            perm.ContainerGUID = "default";
            perm.DeleteContainer = true;
            perm.DeleteObject = true;
            perm.ReadContainer = true;
            perm.ReadObject = true; 
            perm.WriteContainer = true;
            perm.WriteObject = true;
            perm.IsAdmin = true;
            perm.ApiKeyGUID = apiKey.GUID; 
            perm.Notes = "Created by setup script";
            perm.Active = true;
            perm = orm.Insert<Permission>(perm); 

            Console.WriteLine("| Creating container [default]");

            string htmlFile = SampleHtmlFile("http://github.com/jchristn/kvpbase");
            string textFile = SampleTextFile("http://github.com/jchristn/kvpbase");
            string jsonFile = SampleJsonFile("http://github.com/jchristn/kvpbase");

            ContainerManager containerMgr = new ContainerManager(settings, logging, orm);

            Container container = new Container();
            container.UserGUID = "default";
            container.Name = "default";
            container.GUID = "default";
            container.ObjectsDirectory = settings.Storage.Directory + container.UserGUID + "/" + container.Name + "/";
            container.EnableAuditLogging = true;
            container.IsPublicRead = true;
            container.IsPublicWrite = false; 
            containerMgr.Add(container); 

            ContainerClient client = containerMgr.GetContainerClient("default", "default");

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
            Console.WriteLine("");

            ConsoleColor prior = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("IMPORTANT: The default API key setup creates has administrative privileges and");
            Console.WriteLine("can use admin APIs.  We recommend you modify your configuration and reduce its");
            Console.WriteLine("permissions before exposing Kvpbase outside of localhost.");
            Console.ForegroundColor = prior;
            Console.WriteLine("");
            Console.WriteLine("  API key: " + apiKey.GUID); 
            Console.WriteLine("");
            Console.WriteLine("We've also created sample files for you so that you can see your node in");
            Console.WriteLine("action.  Go to the following URLs in your browser and see what happens!");
            Console.WriteLine("");
            Console.WriteLine("  http://localhost:8000/");
            Console.WriteLine("  http://localhost:8000/default/default?_container&html");
            Console.WriteLine("  http://localhost:8000/default/default/hello.html");
            Console.WriteLine("  http://localhost:8000/default/default/hello.html?metadata");
            Console.WriteLine("  http://localhost:8000/default/default/hello.txt");
            Console.WriteLine("  http://localhost:8000/default/default/hello.json"); 
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
    }
}