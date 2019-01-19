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
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static Settings _Settings;
        public static LoggingModule _Logging;
        public static UserManager _UserMgr;
        public static ApiKeyManager _ApiKeyMgr;
        
        public static ContainerManager _ContainerMgr;
        public static ContainerHandler _ContainerHandler;
        public static UrlLockManager _UrlLockMgr;
        public static ObjectHandler _ObjectHandler;
        public static InboundMessageHandler _InboundMessageHandler;

        public static MessageManager _MessageMgr;
        public static TopologyManager _TopologyMgr;
        public static TaskManager _Tasks = null; 
        public static OutboundMessageHandler _OutboundMessageHandler; 

        public static ConnectionManager _ConnMgr; 
        public static EncryptionManager _EncryptionMgr;
        public static TokenManager _TokenMgr;
        public static ResyncManager _ResyncMgr;
        public static ConsoleManager _ConsoleMgr; 
        public static ConcurrentQueue<Dictionary<string, object>> _FailedRequests;
        public static Server _Server;

        public static string _TimestampFormat = "yyyy-MM-ddTHH:mm:ss.ffffffZ";

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
            // Authentication, state, and encrypted related managers
            //

            _UserMgr = new UserManager(_Settings, _Logging);

            _ApiKeyMgr = new ApiKeyManager(_Settings, _Logging, _UserMgr);

            _ConnMgr = new ConnectionManager();

            _EncryptionMgr = new EncryptionManager(_Settings, _Logging);

            _TokenMgr = new TokenManager(_Settings, _Logging, _EncryptionMgr, _UserMgr);

            //
            // Managers and handlers for containers, objects
            //

            _ContainerMgr = new ContainerManager(_Settings.Files.Container, _Settings.Container.CacheSize, _Settings.Container.EvictSize);

            _ContainerHandler = new ContainerHandler(_Settings, _Logging, _ContainerMgr);

            _UrlLockMgr = new UrlLockManager(_Logging);

            _ObjectHandler = new ObjectHandler(_Settings, _Logging, _UrlLockMgr);

            //
            // Managers and handlers for the topology, messaging, and callbacks for messages
            //

            _InboundMessageHandler = new InboundMessageHandler(_Settings, _Logging, _ContainerHandler, _ObjectHandler);

            _MessageMgr = new MessageManager(_Settings, _Logging, _InboundMessageHandler);

            _TopologyMgr = new TopologyManager(_Settings, _Logging, _UserMgr, _MessageMgr);

            _Tasks = new TaskManager(_Settings, _Logging, _TopologyMgr);

            _OutboundMessageHandler = new OutboundMessageHandler(_Settings, _Logging, _TopologyMgr, _Tasks);

            _ResyncMgr = new ResyncManager(_Settings, _Logging, _TopologyMgr, _OutboundMessageHandler, _ContainerMgr, _ContainerHandler, _ObjectHandler);

            //
            // Miscellaneous
            //

            new FailedRequestsThread(_Settings, _Logging, _FailedRequests); 
               
            _Server = new Server(
                _TopologyMgr.LocalNode.Http.DnsHostname, 
                _TopologyMgr.LocalNode.Http.Port, 
                _TopologyMgr.LocalNode.Http.Ssl, 
                RequestReceived, 
                false); 
             
            if (Common.IsTrue(_Settings.EnableConsole))
            {
                _ConsoleMgr = new ConsoleManager(
                    _Settings,
                    _Logging,
                    _TopologyMgr, 
                    _UserMgr,
                    _UrlLockMgr,
                    _EncryptionMgr,
                    _OutboundMessageHandler,
                    _ContainerMgr,
                    _ContainerHandler,
                    _ObjectHandler,
                    _ResyncMgr,
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
            }
            while (!waitHandleSignal);
             
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
                "  " + _Settings.ProductName + " v" + _Settings.ProductVersion + Environment.NewLine +
                Environment.NewLine;

            Console.WriteLine(msg);
        }

        static void CreateDirectories()
        {
            if (!Directory.Exists(_Settings.Storage.Directory)) Directory.CreateDirectory(_Settings.Storage.Directory);
            if (!Directory.Exists(_Settings.Messages.Directory)) Directory.CreateDirectory(_Settings.Messages.Directory);
            if (!Directory.Exists(_Settings.Expiration.Directory)) Directory.CreateDirectory(_Settings.Expiration.Directory);
            if (!Directory.Exists(_Settings.Replication.Directory)) Directory.CreateDirectory(_Settings.Replication.Directory);
            if (!Directory.Exists(_Settings.Tasks.Directory)) Directory.CreateDirectory(_Settings.Tasks.Directory);  
        }

        static HttpResponse RequestReceived(HttpRequest req)
        {
            HttpResponse resp = new HttpResponse(req, false, 500, null, "application/json",
                new ErrorResponse(4, 500, null, null), true);

            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Variables

                DateTime startTime = DateTime.Now;
                
                string apiKey = "";
                string email = "";
                string password = "";
                string token = "";
                string version = "";

                UserMaster currUserMaster = null;
                ApiKey currApiKey = null;
                ApiKeyPermission currPermission = null;

                RequestMetadata md = new RequestMetadata();
                md.Http = req;
                md.Node = _TopologyMgr.LocalNode;
                md.User = null;
                md.Key = null;
                md.Perm = null;

                if (Common.IsTrue(_Settings.Syslog.LogHttpRequests))
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "RequestReceived request received: " + Environment.NewLine + md.Http.ToString());
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
                        DefaultPage("http://github.com/kvpbase"), true);
                    return resp;
                }

                #endregion

                #region Add-Connection

                _ConnMgr.Add(Thread.CurrentThread.ManagedThreadId, req);

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
                                new ErrorResponse(3, 401, "No API key specified.", null), true);
                            return resp;
                        }

                        if (String.Compare(_Settings.Server.AdminApiKey, apiKey) != 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived admin API requested but invalid API key specified");
                            resp = new HttpResponse(req, false, 401, null, "application/json",
                                new ErrorResponse(3, 401, null, null), true);
                            return resp;
                        }

                        resp = AdminApiHandler(md);
                        return resp;
                    }
                }

                #endregion

                #region Authenticate-and-Build-Metadata
                
                if (!String.IsNullOrEmpty(token))
                {
                    if (!_TokenMgr.VerifyToken(token, out currUserMaster, out currPermission))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived unable to verify token");
                        resp = new HttpResponse(req, false, 401, null, "application/json",
                            new ErrorResponse(3, 401, null, null), true);
                        return resp;
                    }
                }
                else if (!String.IsNullOrEmpty(apiKey))
                { 
                    if (!_ApiKeyMgr.VerifyApiKey(apiKey, out currUserMaster, out currApiKey, out currPermission))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived unable to verify API key " + apiKey);
                        resp = new HttpResponse(req, false, 401, null, "application/json",
                           new ErrorResponse(3, 401, null, null), true);
                        return resp;
                    }
                }
                else if ((!String.IsNullOrEmpty(email)) && (!String.IsNullOrEmpty(password)))
                {
                    if (!_UserMgr.Authenticate(email, password, out currUserMaster))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "RequestReceived unable to verify credentials for email " + email);
                        resp = new HttpResponse(req, false, 401, null, "application/json",
                            new ErrorResponse(3, 401, null, null), true);
                        return resp;
                    }

                    currPermission = ApiKeyPermission.DefaultPermit(currUserMaster);
                } 

                md.User = currUserMaster;
                md.Key = currApiKey;
                md.Perm = currPermission;
                md.Params = new RequestMetadata.Parameters();

                md.Params.UserGuid = "null";
                if (md.User != null) md.Params.UserGuid = md.User.Guid;
                if (req.RawUrlEntries.Count >= 1) md.Params.UserGuid = req.RawUrlEntries[0];

                if (req.RawUrlEntries.Count > 1) md.Params.Container = req.RawUrlEntries[1];
                if (req.RawUrlEntries.Count > 2) md.Params.ObjectKey = req.RawUrlEntries[2];

                if (req.QuerystringEntries.ContainsKey("_auditlog"))
                {
                    md.Params.AuditLog = Common.IsTrue(req.QuerystringEntries["_auditlog"]);
                }

                if (req.QuerystringEntries.ContainsKey("_metadata"))
                {
                    md.Params.Metadata = Common.IsTrue(req.QuerystringEntries["_metadata"]);
                }

                if (req.QuerystringEntries.ContainsKey("_reqmetadata"))
                {
                    md.Params.RequestMetadata = Common.IsTrue(req.QuerystringEntries["_reqmetadata"]);
                }

                if (req.QuerystringEntries.ContainsKey("_auditkey"))
                {
                    md.Params.AuditKey = req.QuerystringEntries["_auditkey"];
                }

                if (req.QuerystringEntries.ContainsKey("_action"))
                {
                    md.Params.Action = req.QuerystringEntries["_action"];
                }

                int index; 
                if (req.QuerystringEntries.ContainsKey("_index"))
                {
                    if (Int32.TryParse(req.QuerystringEntries["_index"], out index))
                    {
                        if (index >= 0) md.Params.Index = index;
                    }
                }

                int count; 
                if (req.QuerystringEntries.ContainsKey("_count"))
                {
                    if (Int32.TryParse(req.QuerystringEntries["_count"], out count))
                    {
                        if (count >= 0) md.Params.Count = count;
                    }
                }
                 
                if (req.QuerystringEntries.ContainsKey("_rename"))
                {
                    md.Params.Rename = req.QuerystringEntries["_rename"];
                }
                 
                if (req.QuerystringEntries.ContainsKey("_config"))
                {
                    md.Params.Config = Common.IsTrue(req.QuerystringEntries["_config"]);
                }
                 
                if (req.QuerystringEntries.ContainsKey("_stats"))
                {
                    md.Params.Stats = Common.IsTrue(req.QuerystringEntries["_stats"]);
                }

                DateTime testTimestamp;

                if (req.QuerystringEntries.ContainsKey("_createdbefore"))
                {
                    if (!DateTime.TryParse(req.QuerystringEntries["_createdbefore"], out testTimestamp))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "StorageServer invalid value for _createdbefore");
                        resp = new HttpResponse(req, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for _createdbefore.", null), true);
                    }
                    else
                    {
                        md.Params.CreatedBefore = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_createdafter"))
                {
                    if (!DateTime.TryParse(req.QuerystringEntries["_createdafter"], out testTimestamp))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "StorageServer invalid value for _createdafter");
                        resp = new HttpResponse(req, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for _createdafter.", null), true);
                    }
                    else
                    {
                        md.Params.CreatedAfter = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_updatedbefore"))
                {
                    if (!DateTime.TryParse(req.QuerystringEntries["_updatedbefore"], out testTimestamp))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "StorageServer invalid value for _updatedbefore");
                        resp = new HttpResponse(req, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for _updatedbefore.", null), true);
                    }
                    else
                    {
                        md.Params.UpdatedBefore = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_updatedafter"))
                {
                    if (!DateTime.TryParse(req.QuerystringEntries["_updatedafter"], out testTimestamp))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "StorageServer invalid value for _updatedafter");
                        resp = new HttpResponse(req, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for _updatedafter.", null), true);
                    }
                    else
                    {
                        md.Params.UpdatedAfter = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_accessedbefore"))
                {
                    if (!DateTime.TryParse(req.QuerystringEntries["_accessedbefore"], out testTimestamp))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "StorageServer invalid value for _accessedbefore");
                        resp = new HttpResponse(req, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for _updatedbefore.", null), true);
                    }
                    else
                    {
                        md.Params.LastAccessBefore = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_accessedafter"))
                {
                    if (!DateTime.TryParse(req.QuerystringEntries["_accessedafter"], out testTimestamp))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "StorageServer invalid value for _accessedafter");
                        resp = new HttpResponse(req, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for _updatedafter.", null), true);
                    }
                    else
                    {
                        md.Params.LastAccessAfter = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_md5"))
                {
                    md.Params.Md5 = req.QuerystringEntries["_md5"];
                }

                if (req.QuerystringEntries.ContainsKey("_orderby"))
                {
                    md.Params.OrderBy = req.QuerystringEntries["_orderby"];
                }

                if (req.QuerystringEntries.ContainsKey("_contenttype"))
                {
                    md.Params.ContentType = req.QuerystringEntries["_contenttype"];
                }

                if (req.QuerystringEntries.ContainsKey("_tags"))
                {
                    md.Params.Tags = req.QuerystringEntries["_tags"];
                }

                long testLong = 0;
                if (req.QuerystringEntries.ContainsKey("_sizemin"))
                {
                    if (!Int64.TryParse(req.QuerystringEntries["_sizemin"], out testLong))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "StorageServer invalid value for _sizemin");
                        resp = new HttpResponse(req, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for _sizemin.", null), true);
                    }
                    else
                    {
                        md.Params.SizeMin = testLong;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_sizemax"))
                {
                    if (!Int64.TryParse(req.QuerystringEntries["_sizemax"], out testLong))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "StorageServer invalid value for _sizemax");
                        resp = new HttpResponse(req, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for _sizemax.", null), true);
                    }
                    else
                    {
                        md.Params.SizeMax = testLong;
                    }
                }

                _ConnMgr.Update(Thread.CurrentThread.ManagedThreadId, md.User);

                #endregion 

                #region Call-User-API

                resp = UserApiHandler(md);
                return resp;

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("StorageServer", "RequestReceived", e);
                resp = new HttpResponse(req, false, 500, null, "application/json",
                    new ErrorResponse(1, 500, "Outer exception.", null), true);
                return resp;
            }
            finally
            {
                sw.Stop();

                _ConnMgr.Close(Thread.CurrentThread.ManagedThreadId);

                string msg =
                    req.SourceIp + ":" + req.SourcePort + " " +
                    req.Method + " " + req.RawUrlWithoutQuery + " " +
                    resp.StatusCode + " " +
                    "[" + sw.ElapsedMilliseconds + "ms]";
                _Logging.Log(LoggingModule.Severity.Info, msg);

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

            if (Common.IsTrue(_TopologyMgr.LocalNode.Http.Ssl))
            {
                responseHeaders.Add("Host", "https://" + _TopologyMgr.LocalNode.Http.DnsHostname + ":" + _TopologyMgr.LocalNode.Http.Port);
            }
            else
            {
                responseHeaders.Add("Host", "http://" + _TopologyMgr.LocalNode.Http.DnsHostname + ":" + _TopologyMgr.LocalNode.Http.Port);
            }

            _Logging.Log(LoggingModule.Severity.Debug, "OptionsHandler " + Thread.CurrentThread.ManagedThreadId + ": exiting successfully from OptionsHandler");
            return new HttpResponse(req, true, 200, responseHeaders, null, null, true);
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

        static bool ExitApplication()
        {
            _Logging.Log(LoggingModule.Severity.Info, "StorageServer exiting due to console request");
            Environment.Exit(0);
            return true;
        }
    }
}
