using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks; 
using DatabaseWrapper;
using SyslogLogging;
using Watson.ORM;
using Watson.ORM.Core;
using WatsonWebserver; 
using Kvpbase.StorageServer.Classes.DatabaseObjects;
using Kvpbase.StorageServer.Classes.Handlers;
using Kvpbase.StorageServer.Classes.Managers; 
using Kvpbase.StorageServer.Classes;

using Common = Kvpbase.StorageServer.Classes.Common;

namespace Kvpbase.StorageServer
{
    /// <summary>
    /// Kvpbase Storage Server.
    /// </summary>
    public partial class Program
    {
        private static Settings _Settings;
        private static LoggingModule _Logging;
        private static WatsonORM _ORM;
        private static AuthManager _AuthMgr;
        private static LockManager _LockMgr;

        private static ContainerManager _ContainerMgr;
        private static ObjectHandler _ObjectHandler;
        private static ConnectionManager _ConnMgr;
        private static ConsoleManager _ConsoleMgr; 
        private static Server _Server;

        private static string _TimestampFormat = "yyyy-MM-ddTHH:mm:ss.ffffffZ";
        private static string _Version = null;
        private static string _Header = "[Kvpbase] ";

        /// <summary>
        /// Main method.
        /// </summary>
        /// <param name="args">Command line arguments.</param>
        public static void Main(string[] args)
        {
            _Version = Assembly.GetExecutingAssembly().GetName().Version.ToString();

            bool initialSetup = false;
            if (args != null && args.Length >= 1)
            {
                if (String.Compare(args[0], "setup") == 0) initialSetup = true;
            }

            if (!Common.FileExists("System.json")) initialSetup = true;
            if (initialSetup)
            {
                Setup setup = new Setup();
            }

            _Settings = Settings.FromFile("System.json");

            Welcome(initialSetup);
            CreateDirectories();
            InitializeGlobals();

            if (_Settings.EnableConsole && Environment.UserInteractive)
            {
                _ConsoleMgr.Worker();
            }
            else
            {
                EventWaitHandle waitHandle = new EventWaitHandle(false, EventResetMode.AutoReset, null);
                bool waitHandleSignal = false;
                do
                {
                    waitHandleSignal = waitHandle.WaitOne(1000);
                }
                while (!waitHandleSignal);
            }

            _Logging.Debug(_Header + "exiting"); 
        }

        private static void Welcome(bool skipLogo)
        {
            // http://patorjk.com/software/taag/#p=display&f=Small&t=kvpbase

            ConsoleColor prior = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.DarkGray;

            if (!skipLogo) Console.WriteLine(Logo()); 
            Console.WriteLine("Kvpbase | Object storage platform | v" + _Version);
            Console.WriteLine("");
            Console.ForegroundColor = ConsoleColor.Gray;
             
            if (_Settings.Server.DnsHostname.Equals("localhost") || _Settings.Server.DnsHostname.Equals("127.0.0.1"))
            {
                //                          1         2         3         4         5         6         7         8
                //                 12345678901234567890123456789012345678901234567890123456789012345678901234567890
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("WARNING: Kvpbase started on '" + _Settings.Server.DnsHostname + "'");
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                Console.WriteLine("Kvpbase can only service requests from the local machine.  If you wish to serve");
                Console.WriteLine("external requests, edit the System.json file and specify a DNS-resolvable");
                Console.WriteLine("hostname in the Server.DnsHostname field.");
                Console.WriteLine("");
            }

            List<string> adminListeners = new List<string> { "*", "+", "0.0.0.0" };

            if (adminListeners.Contains(_Settings.Server.DnsHostname))
            {
                //                          1         2         3         4         5         6         7         8
                //                 12345678901234567890123456789012345678901234567890123456789012345678901234567890
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("NOTICE: Kvpbase listening on a wildcard hostname: '" + _Settings.Server.DnsHostname + "'");
                Console.ForegroundColor = ConsoleColor.DarkCyan;
                Console.WriteLine("Kvpbase must be run with administrative privileges, otherwise it will not be");
                Console.WriteLine("able to respond to incoming requests.");
                Console.WriteLine("");
            }

            Console.ForegroundColor = prior;
        }

        private static void CreateDirectories()
        {
            if (!Directory.Exists(_Settings.Storage.Directory)) Directory.CreateDirectory(_Settings.Storage.Directory);
            if (!Directory.Exists(_Settings.Syslog.LogDirectory)) Directory.CreateDirectory(_Settings.Syslog.LogDirectory); 
        }

        private static void InitializeGlobals()
        {
            ConsoleColor previous = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.DarkGray;

            Console.Write("| Initializing logging                : ");
            _Logging = new LoggingModule(
                _Settings.Syslog.ServerIp,
                _Settings.Syslog.ServerPort,
                _Settings.Syslog.ConsoleLogging,
                (Severity)_Settings.Syslog.MinimumLevel,
                false,
                true,
                true,
                false,
                false,
                false);
            Console.WriteLine("[success]");

            if (_Settings.Syslog.FileLogging)
            {
                _Logging.FileLogging = FileLoggingMode.FileWithDate;
                _Logging.LogFilename = "Kvpbase.StorageServer.Log";
            }

            Console.Write("| Initializing database               : ");
            _ORM = new WatsonORM(_Settings.Database);
            _ORM.InitializeDatabase();
            _ORM.InitializeTable(typeof(ApiKey));
            _ORM.InitializeTable(typeof(AuditLogEntry));
            _ORM.InitializeTable(typeof(Container));
            _ORM.InitializeTable(typeof(ContainerKeyValuePair));
            _ORM.InitializeTable(typeof(ObjectKeyValuePair));
            _ORM.InitializeTable(typeof(ObjectMetadata));
            _ORM.InitializeTable(typeof(Permission));
            _ORM.InitializeTable(typeof(UrlLock));
            _ORM.InitializeTable(typeof(UserMaster));
            Console.WriteLine("[success]");
             
            Console.Write("| Initializing authentication manager : ");
            _AuthMgr = new AuthManager(_Settings, _Logging, _ORM);
            Console.WriteLine("[success]");

            Console.Write("| Initializing lock manager           : ");
            _LockMgr = new LockManager(_Settings, _Logging, _ORM);
            Console.WriteLine("[success]");

            Console.Write("| Initializing connection manager     : ");
            _ConnMgr = new ConnectionManager();
            Console.WriteLine("[success]");

            Console.Write("| Initializing container manager      : ");
            _ContainerMgr = new ContainerManager(_Settings, _Logging, _ORM);
            Console.WriteLine("[success]");

            Console.Write("| Initializing object handler         : ");
            _ObjectHandler = new ObjectHandler(_Settings, _Logging, _ORM, _LockMgr);
            Console.WriteLine("[success]");

            Console.Write("| Initializing webserver              : ");
            _Server = new Server(
                _Settings.Server.DnsHostname,
                _Settings.Server.Port,
                _Settings.Server.Ssl,
                RequestReceived);
            _Server.Events.ExceptionEncountered = WebserverException;
            Console.WriteLine("[success]");
            if (_Settings.Server.Ssl)
            {
                Console.WriteLine("| https://" + _Settings.Server.DnsHostname + ":" + _Settings.Server.Port);
            }
            else
            {
                Console.WriteLine("| http://" + _Settings.Server.DnsHostname + ":" + _Settings.Server.Port);
            }

            _ConsoleMgr = new ConsoleManager(
                _Settings,
                _Logging,
                _ContainerMgr,
                _ObjectHandler);

            Console.WriteLine("");
            Console.ForegroundColor = previous;
        }

        private static async Task RequestReceived(HttpContext ctx)
        { 
            string header = _Header + ctx.Request.SourceIp + ":" + ctx.Request.SourcePort + " ";

            DateTime startTime = DateTime.Now;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            RequestMetadata md = new RequestMetadata();
            md.Http = ctx;
            md.User = null;
            md.Key = null;
            md.Perm = null;

            try
            {  
                if (Common.IsTrue(_Settings.Debug.HttpRequest)) 
                    _Logging.Debug(header + "RequestReceived request received: " + Environment.NewLine + md.Http.ToString()); 
                 
                if (ctx.Request.Method == HttpMethod.OPTIONS)
                {
                    await OptionsHandler(ctx);
                    return;
                }
                 
                if (ctx.Request.RawUrlEntries != null && ctx.Request.RawUrlEntries.Count > 0)
                {
                    if (ctx.Request.RawUrlEntries[0].Equals("favicon.ico"))
                    {
                        ctx.Response.StatusCode = 200;
                        await ctx.Response.Send(File.ReadAllBytes("./Assets/favicon.ico"));
                        return;
                    }

                    if (ctx.Request.RawUrlEntries[0].Equals("robots.txt"))
                    {
                        ctx.Response.StatusCode = 200;
                        ctx.Response.ContentType = "text/plain";
                        await ctx.Response.Send("User-Agent: *\r\nDisallow:\r\n");
                        return;
                    }
                }

                if (ctx.Request.RawUrlEntries == null || ctx.Request.RawUrlEntries.Count == 0)
                {
                    ctx.Response.StatusCode = 200;
                    ctx.Response.ContentType = "text/html";
                    await ctx.Response.Send(DefaultPage("http://github.com/kvpbase"));
                    return;
                }
                 
                _ConnMgr.Add(Thread.CurrentThread.ManagedThreadId, ctx);
                 
                string apiKeyVal = ctx.Request.RetrieveHeaderValue(_Settings.Server.HeaderApiKey);
                UserMaster user = null;
                ApiKey apiKey = null;
                AuthResult authResult = AuthResult.None;
                Permission effectivePermissions = null;

                if (!String.IsNullOrEmpty(apiKeyVal))
                { 
                    if (!_AuthMgr.Authenticate(apiKeyVal, out user, out apiKey, out effectivePermissions, out authResult))
                    {
                        _Logging.Warn("RequestReceived unable to verify API key " + apiKeyVal + ": " + authResult);
                        ctx.Response.StatusCode = 401;
                        ctx.Response.ContentType = "application/json";
                        await ctx.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                        return;
                    }
                }
                 
                md.User = user;
                md.Key = apiKey;
                md.Perm = effectivePermissions; 
                md.Params = RequestMetadata.Parameters.FromHttpRequest(ctx.Request);
                if (md.User != null) md.Params.UserGUID = md.User.GUID; 
                _ConnMgr.Update(Thread.CurrentThread.ManagedThreadId, md.User);
                 
                if (ctx.Request.RawUrlEntries != null
                    && ctx.Request.RawUrlEntries.Count >= 2
                    && ctx.Request.RawUrlEntries[0].Equals("admin")
                    && md.Perm.IsAdmin)
                {
                    await AdminApiHandler(md);
                }
                else
                {
                    await UserApiHandler(md);
                }

                return;
            }
            catch (Exception e)
            {
                _Logging.Exception("StorageServer", "RequestReceived", e); 
                ctx.Response.StatusCode = 500;
                ctx.Response.ContentType = "application/json";
                await ctx.Response.Send(Common.SerializeJson(new ErrorResponse(1, 500, "Outer exception.", null), true));
                return;
            }
            finally
            {
                sw.Stop();

                _ConnMgr.Close(Thread.CurrentThread.ManagedThreadId);

                string msg =
                    header + 
                    ctx.Request.Method + " " + ctx.Request.RawUrlWithoutQuery + " " +
                    ctx.Response.StatusCode + " " +
                    "[" + sw.ElapsedMilliseconds + "ms]";

                _Logging.Debug(msg); 
            }
        }

        private static async Task OptionsHandler(HttpContext ctx)
        { 
            Dictionary<string, string> responseHeaders = new Dictionary<string, string>();

            string[] requestedHeaders = null;
            if (ctx.Request.Headers != null)
            {
                foreach (KeyValuePair<string, string> curr in ctx.Request.Headers)
                {
                    if (String.IsNullOrEmpty(curr.Key)) continue;
                    if (String.IsNullOrEmpty(curr.Value)) continue;
                    if (String.Compare(curr.Key.ToLower(), "access-control-request-headers") == 0)
                    {
                        requestedHeaders = curr.Value.Split(',');
                        break;
                    }
                }
            }

            string headers = _Settings.Server.HeaderApiKey;

            if (requestedHeaders != null)
            {
                foreach (string curr in requestedHeaders)
                {
                    headers += ", " + curr;
                }
            }

            responseHeaders.Add("Access-Control-Allow-Methods", "OPTIONS, HEAD, GET, PUT, POST, DELETE");
            responseHeaders.Add("Access-Control-Allow-Headers", "*, Content-Type, X-Requested-With, " + headers);
            responseHeaders.Add("Access-Control-Expose-Headers", "Content-Type, X-Requested-With, " + headers);
            responseHeaders.Add("Access-Control-Allow-Origin", "*");
            responseHeaders.Add("Accept", "*/*");
            responseHeaders.Add("Accept-Language", "en-US, en");
            responseHeaders.Add("Accept-Charset", "ISO-8859-1, utf-8");
            responseHeaders.Add("Connection", "keep-alive");

            if (_Settings.Server.Ssl)
            {
                responseHeaders.Add("Host", "https://" + _Settings.Server.DnsHostname + ":" + _Settings.Server.Port);
            }
            else
            {
                responseHeaders.Add("Host", "http://" + _Settings.Server.DnsHostname + ":" + _Settings.Server.Port);
            }

            ctx.Response.StatusCode = 200;
            ctx.Response.Headers = responseHeaders;
            await ctx.Response.Send();
        }

        private static void WebserverException(string ip, int port, Exception e)
        {
            _Logging.Exception("StorageServer", "Webserver [" + ip + ":" + port + "]", e); 
        }

        private static string DefaultPage(string link)
        {
            string html =
                "<html>" + Environment.NewLine +
                "   <head>" + Environment.NewLine +
                "      <title>Welcome to Kvpbase!</title>" + Environment.NewLine +
                "      <style>" + Environment.NewLine +
                "          body {" + Environment.NewLine +
                "            font-family: arial;" + Environment.NewLine +
                "          }" + Environment.NewLine +
                "          pre {" + Environment.NewLine +
                "            background-color: #e5e7ea;" + Environment.NewLine +
                "            color: #333333; " + Environment.NewLine +
                "          }" + Environment.NewLine +
                "          h3 {" + Environment.NewLine +
                "            color: #333333; " + Environment.NewLine +
                "            padding: 4px;" + Environment.NewLine +
                "            border: 4px;" + Environment.NewLine +
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
                "      <pre>" + Environment.NewLine +
                WebUtility.HtmlEncode(Logo()) +
                "      </pre>" + Environment.NewLine +
                "      <p>Congratulations, your Kvpbase Storage Server node is running!</p>" + Environment.NewLine +
                "      <p>" + Environment.NewLine + 
                "        <a href='" + link + "' target='_blank'>SDKs and Source Code</a>" + Environment.NewLine +
                "      </p>" + Environment.NewLine +
                "   </body>" + Environment.NewLine +
                "</html>";

            return html;
        }

        private static string Logo()
        {
            // http://patorjk.com/software/taag/#p=display&f=Small&t=kvpbase

            string ret =
                Environment.NewLine +
                @"   _             _                    " + Environment.NewLine +
                @"  | |____ ___ __| |__  __ _ ___ ___   " + Environment.NewLine +
                @"  | / /\ V / '_ \ '_ \/ _` (_-</ -_)  " + Environment.NewLine +
                @"  |_\_\ \_/| .__/_.__/\__,_/__/\___|  " + Environment.NewLine +
                @"           |_|                        " + Environment.NewLine +
                @"                                      " + Environment.NewLine +
                Environment.NewLine;

            return ret;
        }

        private static bool ExitApplication()
        {
            _Logging.Info(_Header + "exiting due to console request");
            Environment.Exit(0);
            return true;
        }
    }
}
