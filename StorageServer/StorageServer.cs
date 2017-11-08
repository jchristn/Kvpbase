using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static Settings _Settings;
        public static Events _Logging;
        public static UserManager _Users;
        public static ApiKeyManager _ApiKeys;
        public static Topology _Topology;
        public static Node _Node;
        public static ConnectionManager _ConnMgr;
        public static MessageManager _MessageMgr;
        public static EncryptionModule _EncryptionMgr;
        public static TokenManager _Tokens;
        public static UrlLockManager _UrlLockMgr;
        public static LoggerManager _Logger;
        public static MaintenanceManager _MaintenanceMgr;
        public static ConsoleManager _ConsoleMgr;
        public static ConcurrentQueue<Dictionary<string, object>> _FailedRequests;
        public static Server _Server;

        public static BunkerHandler _Bunker;
        public static ReplicationHandler _Replication;
        public static ObjectHandler _Object;
        public static ContainerHandler _Container;

        public static void Main(string[] args)
        {
            #region Startup-Check

            if (!Common.IsAdmin())
            {
                Common.ExitApplication("StorageServer", "This application must be run with administrative privileges", -1);
                return;
            }

            if (!HttpListener.IsSupported)
            {
                Common.ExitApplication("StorageServer", "Your OS does not support HttpListener", -1);
                return;
            }

            #endregion

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

            #endregion

            #region Verify-Storage-Directory-Access

            if (!Common.VerifyDirectoryAccess(_Settings.Environment, _Settings.Storage.Directory))
            {
                Common.ExitApplication("StorageServer", "Unable to verify storage directory access", -1);
                return;
            }

            #endregion

            #region Initialize-Global-Variables

            _Logging = new Events(_Settings);
            _Users = new UserManager(_Logging, UserMaster.FromFile(_Settings.Files.UserMaster));
            _ApiKeys = new ApiKeyManager(_Logging, ApiKey.FromFile(_Settings.Files.ApiKey), ApiKeyPermission.FromFile(_Settings.Files.Permission));
            _Topology = Topology.FromFile(_Settings.Files.Topology);
            _ConnMgr = new ConnectionManager();
            _MessageMgr = new MessageManager(_Settings, _Logging);
            _EncryptionMgr = new EncryptionModule(_Settings, _Logging);
            _Tokens = new TokenManager(_Settings, _Logging, _EncryptionMgr, _Users);
            _UrlLockMgr = new UrlLockManager(_Logging);
            _Logger = new LoggerManager(_Settings, _Logging);
            _MaintenanceMgr = new MaintenanceManager(_Logging);

            #endregion

            #region Verify-Topology

            if (!_Topology.ValidateTopology(out _Node))
            {
                Common.ExitApplication("StorageServer", "Topology errors detected", -1);
                return;
            }

            _Topology.PopulateReplicas(_Node);
            Console.WriteLine("Populated " + _Topology.Replicas.Count + " replica node(s) in topology");

            #endregion

            #region Initialize-Handlers

            _Bunker = new BunkerHandler(_Settings, _Logging);
            _Replication = new ReplicationHandler(_Settings, _Logging, _MessageMgr, _Topology, _Node, _Users);
            _Object = new ObjectHandler(_Settings, _Logging, _MessageMgr, _Topology, _Node, _Users, _UrlLockMgr, _MaintenanceMgr, _EncryptionMgr, _Logger, _Bunker, _Replication);
            _Container = new ContainerHandler(_Settings, _Logging, _MessageMgr, _Topology, _Node, _Users, _MaintenanceMgr, _Logger, _Bunker, _Replication);

            #endregion

            #region Start-Threads

            new PublicObjThread(_Settings, _Logging);
            new FailedRequestsThread(_Settings, _Logging, _FailedRequests);
            new PeerManagerThread(_Settings, _Logging, _Topology, _Node);
            new MessengerThread(_Settings, _Logging, _Topology, _Node);
            new ExpirationThread(_Settings, _Logging);
            new ReplicationThread(_Settings, _Logging, _Topology, _Node);
            new TasksThread(_Settings, _Logging);

            _Server = new Server(_Node.DnsHostname, _Node.Port, Common.IsTrue(_Node.Ssl), RequestReceived, false); 

            #endregion

            #region Console

            if (Common.IsTrue(_Settings.EnableConsole))
            {
                _ConsoleMgr = new ConsoleManager(
                    _Settings,
                    _MaintenanceMgr,
                    _Topology,
                    _Node,
                    _Users,
                    _UrlLockMgr,
                    _EncryptionMgr,
                    _Logging,
                    ExitApplication);
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Debug, "StorageServer not using interactive mode, console disabled");
            }

            #endregion

            #region Wait-for-Server-Thread
            
            EventWaitHandle waitHandle = new EventWaitHandle(false, EventResetMode.AutoReset, Guid.NewGuid().ToString());
            bool waitHandleSignal = false;
            do
            {
                waitHandleSignal = waitHandle.WaitOne(1000);
            } while (!waitHandleSignal);
             
            _Logging.Log(LoggingModule.Severity.Debug, "StorageServer exiting");

            #endregion
        }
        
        static void Welcome()
        {
            // http://patorjk.com/software/taag/#p=display&f=Small&t=kvpbase

            string msg =
                Environment.NewLine +
                @"   _             _                    " + Environment.NewLine +
                @"  | |____ ___ __| |__  __ _ ___ ___   " + Environment.NewLine +
                @"  | / /\ V / '_ \ '_ \/ _` (_-</ -_)  " + Environment.NewLine +
                @"  |_\_\ \_/| .__/_.__/\__,_/__/\___|  " + Environment.NewLine +
                @"           |_|                        " + Environment.NewLine +
                @"                                      " + Environment.NewLine +
                Environment.NewLine +
                "  " + _Settings.ProductName + " :: v" + _Settings.ProductVersion + Environment.NewLine +
                Environment.NewLine;

            Console.WriteLine(msg);
        }

        static HttpResponse RequestReceived(HttpRequest req)
        {
            HttpResponse resp = new HttpResponse(req, false, 500, null, "application/json", 
                new ErrorResponse(4, 500, null, null).ToJson(),
                true);

            try
            {
                #region Variables

                DateTime startTime = DateTime.Now;
                RequestMetadata md = new RequestMetadata();

                string apiKey = "";
                string email = "";
                string password = "";
                string token = "";
                string version = "";

                UserMaster currUserMaster = null;
                ApiKey currApiKey = null;
                ApiKeyPermission currApiKeyPermission = null;
                
                md.CurrHttpReq = req;
                md.CurrNode = _Node;

                if (Common.IsTrue(_Settings.Syslog.LogHttpRequests))
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "RequestReceived request received: " + Environment.NewLine + md.CurrHttpReq.ToString());
                }

                #endregion

                #region Options-Handler

                if (req.Method.ToLower().Trim().Contains("option"))
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "RequestReceived " + Thread.CurrentThread.ManagedThreadId + ": OPTIONS request received");
                    resp = OptionsHandler(req);
                    return resp;
                }

                #endregion

                #region Favicon-Robots-Root

                if (req.RawUrlEntries != null && req.RawUrlEntries.Count > 0)
                {
                    if (String.Compare(req.RawUrlEntries[0].ToLower(), "favicon.ico") == 0)
                    {
                        resp = new HttpResponse(req, true, 200, null, null, null, true);
                        return resp;
                    }
                
                    if (String.Compare(req.RawUrlEntries[0].ToLower(), "robots.txt") == 0)
                    {
                        resp = new HttpResponse(req, true, 200, null, "text/plain", "User-Agent: *\r\nDisallow:\r\n", true);
                        return resp;
                    }
                }

                if (req.RawUrlEntries == null || req.RawUrlEntries.Count == 0)
                {
                    resp = new HttpResponse(req, true, 200, null, "text/html",
                        DefaultPage("http://www.kvpbase.com/", _Settings.DocumentationUrl, "http://github.com/kvpbase"),
                        true);
                    return resp;
                }

                #endregion

                #region Add-Connection

                _ConnMgr.Add(Thread.CurrentThread.ManagedThreadId, req);

                #endregion

                #region Unauthenticated-API

                switch (req.Method.ToLower())
                {
                    case "get":
                        #region get

                        #region loopback

                        if (WatsonCommon.UrlEqual(req.RawUrlWithoutQuery, "/loopback", false))
                        {
                            return new HttpResponse(req, true, 200, null, "text/plain", "Hello from kvpbase!", true);
                        }

                        #endregion

                        #region public

                        if (req.RawUrlWithoutQuery.StartsWith("/public/"))
                        {
                            resp = GetPublicObject(md);
                            return resp;
                        }

                        #endregion

                        #region version

                        if (WatsonCommon.UrlEqual(req.RawUrlWithoutQuery, "/version", false))
                        {
                            resp = new HttpResponse(req, true, 200, null, "text/plain", _Settings.ProductVersion, true);
                            return resp;
                        }

                        #endregion

                        break;

                    #endregion

                    case "put":
                        break;

                    case "post":
                        break;

                    case "delete":
                        break;

                    default:
                        break;
                }

                #endregion

                #region Retrieve-Auth-Parameters

                apiKey = req.RetrieveHeaderValue(_Settings.Server.HeaderApiKey);
                email = req.RetrieveHeaderValue(_Settings.Server.HeaderEmail);
                password = req.RetrieveHeaderValue(_Settings.Server.HeaderPassword);
                token = req.RetrieveHeaderValue(_Settings.Server.HeaderToken);
                version = req.RetrieveHeaderValue(_Settings.Server.HeaderVersion);

                #endregion

                #region Admin-API
                
                if (req.RawUrlEntries != null && req.RawUrlEntries.Count > 0)
                {
                    if (String.Compare(req.RawUrlEntries[0], "admin") == 0)
                    {
                        if (String.IsNullOrEmpty(apiKey))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived admin API requested but no API key specified");
                            resp = new HttpResponse(req, false, 401, null, "application/json",
                                new ErrorResponse(3, 401, "No API key specified.", null).ToJson(),
                                true);
                            return resp;
                        }

                        if (String.Compare(_Settings.Server.AdminApiKey, apiKey) != 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived admin API requested but invalid API key specified");
                            resp = new HttpResponse(req, false, 401, null, "application/json",
                                new ErrorResponse(3, 401, null, null).ToJson(),
                                true);
                            return resp;
                        }

                        resp = AdminApiHandler(md);
                        return resp;
                    }
                }

                #endregion

                #region Authenticate-Request-and-Update-Connection
                
                if (!String.IsNullOrEmpty(token))
                {
                    if (!_Tokens.VerifyToken(token, out currUserMaster, out currApiKeyPermission))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived unable to verify token");
                        resp = new HttpResponse(req, false, 401, null, "application/json",
                            new ErrorResponse(3, 401, null, null).ToJson(),
                            true);
                        return resp;
                    }
                }
                else if (!String.IsNullOrEmpty(apiKey))
                {
                    if (!_ApiKeys.VerifyApiKey(apiKey, _Users, out currUserMaster, out currApiKey, out currApiKeyPermission))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived unable to verify API key " + apiKey);
                        resp = new HttpResponse(req, false, 401, null, "application/json",
                           new ErrorResponse(3, 401, null, null).ToJson(),
                           true);
                        return resp;
                    }
                }
                else if ((!String.IsNullOrEmpty(email)) && (!String.IsNullOrEmpty(password)))
                {
                    if (!_Users.AuthenticateCredentials(email, password, out currUserMaster))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived unable to verify credentials for email " + email);
                        resp = new HttpResponse(req, false, 401, null, "application/json",
                            new ErrorResponse(3, 401, null, null).ToJson(),
                            true);
                        return resp;
                    }

                    currApiKeyPermission = ApiKeyPermission.DefaultPermit(currUserMaster);
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived user API requested but no authentication material supplied");
                    resp = new HttpResponse(req, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "No authentication material.", null).ToJson(),
                        true);
                    return resp;
                }

                md.CurrUser = currUserMaster;
                md.CurrApiKey = currApiKey;
                md.CurrPerm = currApiKeyPermission;

                _ConnMgr.Update(Thread.CurrentThread.ManagedThreadId, Convert.ToInt32(currUserMaster.UserMasterId), currUserMaster.Email);

                #endregion

                #region User-Administrative-APIs
                
                switch (md.CurrHttpReq.Method.ToLower())
                {
                    case "get":
                        #region get

                        if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/token", false))
                        {
                            resp = new HttpResponse(req, true, 200, null, "text/plain", _Tokens.TokenFromUser(md.CurrUser), true);
                            return resp;
                        }

                        if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/replicas", false))
                        {
                            resp = GetReplicas(md);
                            return resp;
                        }

                        if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/user_master", false))
                        {
                            resp = new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(md.CurrUser), true);
                            return resp;
                        }
                        
                        break;

                    #endregion

                    case "put":
                        #region put

                        break;

                    #endregion

                    case "post":
                        #region post

                        break;

                    #endregion

                    case "delete":
                        #region delete

                        break;

                    #endregion

                    case "head":
                        #region head

                        break;

                    #endregion

                    default:
                        #region default

                        _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived unknown http method: " + md.CurrHttpReq.Method);
                        resp = new HttpResponse(req, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unknown method.", null).ToJson(),
                            true);
                        return resp;

                        #endregion
                }

                #endregion

                #region Build-Object

                md.CurrObj = Obj.BuildObj(md, _Users, _Settings, _Topology, _Node, _Logging);
                if (md.CurrObj == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived unable to build payload object from request");
                    resp = new HttpResponse(req, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to build object from request.", null).ToJson(),
                        true);
                    return resp;
                }

                #endregion

                #region Call-User-API

                resp = UserApiHandler(md);
                return resp;

                #endregion
            }
            catch (Exception e)
            {
                _Logging.Exception("RequestReceived", "Outer exception", e);
                return new HttpResponse(req, false, 500, null, "application/json",
                    new ErrorResponse(1, 500, "Outer exception.", null).ToJson(),
                    true);
            }
            finally
            {
                _ConnMgr.Close(Thread.CurrentThread.ManagedThreadId);

                if (Common.IsTrue(_Settings.Syslog.LogHttpRequests))
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "RequestReceived sending response: " + Environment.NewLine + resp.ToString());
                }
            }
        }

        static HttpResponse OptionsHandler(HttpRequest req)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "OptionsHandler " + Thread.CurrentThread.ManagedThreadId + ": processing options request");

            Dictionary<string, string> responseHeaders = new Dictionary<string, string>();

            string[] requestedHeaders = null;
            if (req.Headers != null)
            {
                foreach (KeyValuePair<string, string> curr in req.Headers)
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
                _Settings.Server.HeaderPassword + ", " +
                _Settings.Server.HeaderToken + ", " +
                _Settings.Server.HeaderVersion;

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

            if (Common.IsTrue(_Node.Ssl))
            {
                responseHeaders.Add("Host", "https://" + _Node.DnsHostname + ":" + _Node.Port);
            }
            else
            {
                responseHeaders.Add("Host", "http://" + _Node.DnsHostname + ":" + _Node.Port);
            }

            _Logging.Log(LoggingModule.Severity.Debug, "OptionsHandler " + Thread.CurrentThread.ManagedThreadId + ": exiting successfully from OptionsHandler");
            return new HttpResponse(req, true, 200, responseHeaders, null, null, true);
        }

        static string DefaultPage(string homepageLink, string docsLink, string sdkLink)
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
                "        <a href='" + docsLink + "' target='_blank'>API Documentation</a>&nbsp;&nbsp;" + Environment.NewLine +
                "        <a href='" + homepageLink + "' target='_blank'>Homepage</a>&nbsp;&nbsp;" + Environment.NewLine +
                "        <a href='" + sdkLink + "' target='_blank'>SDKs and Source Code</a>" + Environment.NewLine +
                "      </p>" + Environment.NewLine +
                "   </body>" + Environment.NewLine +
                "</html>";

            return html;
        }

        static bool ExitApplication()
        {
            _Logging.Log(LoggingModule.Severity.Info, "StorageServer exiting due to console request");
            Environment.Exit(0);
            return true;
        }
    }
}
