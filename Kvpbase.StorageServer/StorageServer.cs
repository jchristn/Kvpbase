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
 
using Kvpbase.Classes.Handlers;
using Kvpbase.Classes.Managers; 
using Kvpbase.Containers;
using Kvpbase.Classes;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static Settings _Settings;
        public static LoggingModule _Logging;
        public static DatabaseClient _ConfigDb;
        public static DatabaseClient _StorageDb;
        public static ConfigManager _ConfigMgr;
         
        public static ContainerManager _ContainerMgr;
        public static ObjectHandler _ObjectHandler;  
        public static ConnectionManager _ConnMgr;  
        public static ConsoleManager _ConsoleMgr; 
        public static ConcurrentQueue<Dictionary<string, object>> _FailedRequests;
        public static Server _Server;

        public static string _TimestampFormat = "yyyy-MM-ddTHH:mm:ss.ffffffZ";
        public static string _Version = null;

        public static void Main(string[] args)
        {  
            #region Load-Settings

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

            #endregion
             
            #region Initialize-Globals

            //
            // Global logging
            //

            _Logging = new LoggingModule(
                _Settings.Syslog.ServerIp,
                _Settings.Syslog.ServerPort,
                _Settings.EnableConsole,
                (LoggingModule.Severity)_Settings.Syslog.MinimumLevel,
                false,
                true,
                true,
                false,
                false,
                false);

            // 
            // Databases
            //

            _ConfigDb = new DatabaseClient(
                _Settings.ConfigDatabase.Type,
                _Settings.ConfigDatabase.Hostname,
                _Settings.ConfigDatabase.Port,
                _Settings.ConfigDatabase.Username,
                _Settings.ConfigDatabase.Password,
                _Settings.ConfigDatabase.InstanceName,
                _Settings.ConfigDatabase.DatabaseName);

            _StorageDb = new DatabaseClient(
                _Settings.StorageDatabase.Type,
                _Settings.StorageDatabase.Hostname,
                _Settings.StorageDatabase.Port,
                _Settings.StorageDatabase.Username,
                _Settings.StorageDatabase.Password,
                _Settings.StorageDatabase.InstanceName,
                _Settings.StorageDatabase.DatabaseName);

            //
            // Authentication, state, and encrypted related managers
            //

            _ConfigMgr = new ConfigManager(_Settings, _Logging, _ConfigDb); 
            _ConnMgr = new ConnectionManager();  

            //
            // Managers and handlers for containers, objects
            //
             
            _ContainerMgr = new ContainerManager(_Settings, _Logging, _ConfigMgr, _StorageDb);  
            _ObjectHandler = new ObjectHandler(_Settings, _Logging, _ConfigMgr);
              
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

            #endregion

            #region Wait-for-Server-Thread

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

            _Logging.Debug("StorageServer exiting");

            #endregion
        }
        
        static void Welcome()
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

        static void CreateDirectories()
        {
            if (!Directory.Exists(_Settings.Storage.Directory)) Directory.CreateDirectory(_Settings.Storage.Directory);
        }

        static async Task RequestReceived(HttpContext ctx)
        { 
            string header = ctx.Request.SourceIp + ":" + ctx.Request.SourcePort + " ";

            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Variables

                DateTime startTime = DateTime.Now;
                 
                RequestMetadata md = new RequestMetadata();
                md.Http = ctx; 
                md.User = null;
                md.Key = null;
                md.Perm = null;

                if (Common.IsTrue(_Settings.Syslog.LogHttpRequests)) 
                    _Logging.Debug(header + "RequestReceived request received: " + Environment.NewLine + md.Http.ToString()); 

                #endregion

                #region Options-Handler

                if (ctx.Request.Method == HttpMethod.OPTIONS)
                {
                    await OptionsHandler(ctx);
                    return;
                }

                #endregion

                #region Favicon-Robots-Root

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

                #endregion

                #region Add-Connection

                _ConnMgr.Add(Thread.CurrentThread.ManagedThreadId, ctx);

                #endregion
                   
                #region Authenticate-and-Build-Metadata

                string apiKeyVal = ctx.Request.RetrieveHeaderValue(_Settings.Server.HeaderApiKey);
                string emailVal = ctx.Request.RetrieveHeaderValue(_Settings.Server.HeaderEmail);
                string passwordVal = ctx.Request.RetrieveHeaderValue(_Settings.Server.HeaderPassword);

                UserMaster user = null;
                ApiKey apiKey = null;
                AuthResult authResult = AuthResult.None;
                Permission effectivePermissions = null;

                if (!String.IsNullOrEmpty(apiKeyVal))
                { 
                    if (!_ConfigMgr.Authenticate(apiKeyVal, out user, out apiKey, out effectivePermissions, out authResult))
                    {
                        _Logging.Warn("RequestReceived unable to verify API key " + apiKeyVal + ": " + authResult);
                        ctx.Response.StatusCode = 401;
                        ctx.Response.ContentType = "application/json";
                        await ctx.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                        return;
                    }
                }
                else if ((!String.IsNullOrEmpty(emailVal)) && (!String.IsNullOrEmpty(passwordVal)))
                {
                    if (!_ConfigMgr.Authenticate(emailVal, passwordVal, out user, out apiKey, out effectivePermissions, out authResult))
                    {
                        _Logging.Warn("RequestReceived unable to verify credentials for email " + emailVal);
                        ctx.Response.StatusCode = 401;
                        ctx.Response.ContentType = "application/json";
                        await ctx.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                        return;
                    }

                    effectivePermissions = Permission.DefaultPermit(user);
                }
                 
                md.User = user;
                md.Key = apiKey;
                md.Perm = effectivePermissions;
                md.Params = RequestMetadata.Parameters.FromHttpRequest(ctx.Request);
                if (md.User != null) md.Params.UserGuid = md.User.GUID; 
                _ConnMgr.Update(Thread.CurrentThread.ManagedThreadId, md.User);

                #endregion

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
                    ctx.Request.SourceIp + ":" + ctx.Request.SourcePort + " " +
                    ctx.Request.Method + " " + ctx.Request.RawUrlWithoutQuery + " " +
                    ctx.Response.StatusCode + " " +
                    "[" + sw.ElapsedMilliseconds + "ms]";

                _Logging.Debug(msg); 
            }
        }

        static async Task OptionsHandler(HttpContext ctx)
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

            string headers =
                _Settings.Server.HeaderApiKey + ", " +
                _Settings.Server.HeaderEmail + ", " +
                _Settings.Server.HeaderPassword;

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

        static bool WebserverException(string ip, int port, Exception e)
        {
            _Logging.Exception("StorageServer", "Webserver [" + ip + ":" + port + "]", e);
            return true;
        }

        static string DefaultPage(string link)
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
        
        static string Logo()
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
         
        static bool ExitApplication()
        {
            _Logging.Info("StorageServer exiting due to console request");
            Environment.Exit(0);
            return true;
        }
    }
}
