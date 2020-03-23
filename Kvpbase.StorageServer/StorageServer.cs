using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks; 
using DatabaseWrapper;
using SyslogLogging;
using WatsonWebserver; 
using Kvpbase.StorageServer.Classes.DatabaseObjects;
using Kvpbase.StorageServer.Classes.Handlers;
using Kvpbase.StorageServer.Classes.Managers; 
using Kvpbase.StorageServer.Classes;

namespace Kvpbase.StorageServer
{
    /// <summary>
    /// Kvpbase Storage Server.
    /// </summary>
    public partial class Program
    {
        private static Settings _Settings;
        private static LoggingModule _Logging;
        private static DatabaseClient _Database;
        private static DatabaseManager _DatabaseMgr;
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

            Welcome();
            CreateDirectories();

            _Logging = new LoggingModule(
                _Settings.Syslog.ServerIp,
                _Settings.Syslog.ServerPort,
                _Settings.EnableConsole,
                (Severity)_Settings.Syslog.MinimumLevel,
                false,
                true,
                true,
                false,
                false,
                false);

            if (_Settings.Syslog.FileLogging)
            {
                _Logging.FileLogging = FileLoggingMode.FileWithDate;
                _Logging.LogFilename = "Kvpbase.StorageServer.Log";
            }

            switch (_Settings.Database.Type)
            {
                case DbTypes.Sqlite:
                    _Database = new DatabaseClient(
                        _Settings.Database.Filename);
                    break;
                case DbTypes.MsSql:
                case DbTypes.MySql:
                case DbTypes.PgSql:
                    _Database = new DatabaseClient(
                        _Settings.Database.Type,
                        _Settings.Database.Hostname,
                        _Settings.Database.Port,
                        _Settings.Database.Username,
                        _Settings.Database.Password,
                        _Settings.Database.InstanceName,
                        _Settings.Database.DatabaseName);
                    break;
                default:
                    throw new ArgumentException("Unknown database type: " + _Settings.Database.Type.ToString());
            }
              
            _DatabaseMgr = new DatabaseManager(_Settings, _Logging, _Database);
            _AuthMgr = new AuthManager(_Settings, _Logging, _DatabaseMgr);
            _LockMgr = new LockManager(_Settings, _Logging, _DatabaseMgr);
            _ConnMgr = new ConnectionManager(); 
            _ContainerMgr = new ContainerManager(_Settings, _Logging, _DatabaseMgr);  
            _ObjectHandler = new ObjectHandler(_Settings, _Logging, _DatabaseMgr, _LockMgr);
              
            _Server = new Server(
                _Settings.Server.DnsHostname,
                _Settings.Server.Port,
                _Settings.Server.Ssl,
                RequestReceived);

            _Server.Events.ExceptionEncountered = WebserverException;
             
            _ConsoleMgr = new ConsoleManager(
                _Settings,
                _Logging, 
                _ContainerMgr, 
                _ObjectHandler);
             
            if (_Settings.EnableConsole)
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

        private static void Welcome()
        {
            // http://patorjk.com/software/taag/#p=display&f=Small&t=kvpbase

            System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
            FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
            _Version = fvi.FileVersion;

            string msg =
                Logo() + 
                Environment.NewLine +
                "  Kvpbase Storage Server v" + _Version + Environment.NewLine +
                Environment.NewLine;

            Console.WriteLine(msg);
        }

        private static void CreateDirectories()
        {
            if (!Directory.Exists(_Settings.Storage.Directory)) Directory.CreateDirectory(_Settings.Storage.Directory);
            if (!Directory.Exists(_Settings.Syslog.LogDirectory)) Directory.CreateDirectory(_Settings.Syslog.LogDirectory); 
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
                    if (ctx.Request.RawUrlWithoutQuery.Equals("/favicon.ico"))
                    {
                        ctx.Response.StatusCode = 200;
                        await ctx.Response.Send();
                        return;
                    }

                    if (ctx.Request.RawUrlWithoutQuery.Equals("/robots.txt"))
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
                if (md.User != null) md.Params.UserGuid = md.User.GUID; 
                _ConnMgr.Update(Thread.CurrentThread.ManagedThreadId, md.User);
                 
                await UserApiHandler(md);
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
                @"                                      " + Environment.NewLine;

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
