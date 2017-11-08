using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using RestWrapper;
using WatsonWebserver;

namespace Kvpbase
{
    public class ContainerHandler
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private Events _Logging;
        private MessageManager _MessageMgr;
        private Topology _Topology;
        private Node _Node;
        private UserManager _UserMgr;
        private MaintenanceManager _MaintenanceMgr;
        private LoggerManager _LoggerMgr;
        private BunkerHandler _Bunker;
        private ReplicationHandler _Replication;

        #endregion

        #region Constructors-and-Factories

        public ContainerHandler(
            Settings settings,
            Events logging,
            MessageManager messages,
            Topology topology,
            Node node,
            UserManager users,
            MaintenanceManager maintenance,
            LoggerManager logger,
            BunkerHandler bunker,
            ReplicationHandler replication)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (messages == null) throw new ArgumentNullException(nameof(messages));
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (users == null) throw new ArgumentNullException(nameof(users));
            if (maintenance == null) throw new ArgumentNullException(nameof(maintenance));
            if (logger == null) throw new ArgumentNullException(nameof(logger));
            if (bunker == null) throw new ArgumentNullException(nameof(bunker));
            if (replication == null) throw new ArgumentNullException(nameof(replication));

            _Settings = settings;
            _Logging = logging;
            _MessageMgr = messages;
            _Topology = topology;
            _Node = node;
            _UserMgr = users;
            _MaintenanceMgr = maintenance;
            _LoggerMgr = logger;
            _Bunker = bunker;
            _Replication = replication;
        }

        #endregion

        #region Public-Methods

        public HttpResponse Delete(RequestMetadata md, bool recursive)
        {
            #region Variables

            bool deleteSuccess = false;
            RestResponse proxyResponse = new RestResponse();
            Dictionary<string, string> restHeaders = new Dictionary<string, string>();
            string containerLogFile = "";
            string containerPropertiesFile = "";
            ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();

            #endregion

            #region Check-Permissions

            if (md.CurrPerm == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete null ApiKeyPermission object supplied");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
            }

            if (!Common.IsTrue(md.CurrPerm.AllowDeleteContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete AllowDeleteContainer operation not authorized per permissions");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
            }

            #endregion

            #region Process-Owner

            if (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId)
            {
                #region Local-Owner

                #region Check-Container-Permissions-and-Logging

                currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                if (currContainerPropertiesFile != null)
                {
                    if (currContainerPropertiesFile.Logging != null)
                    {
                        if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.DeleteContainer))
                            {
                                #region Process-Logging

                                _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "DeleteContainer", null));

                                #endregion
                            }
                        }
                    }

                    if (currContainerPropertiesFile.Permissions != null)
                    {
                        #region Evaluate-Permissions

                        if (!ContainerPermission.GetPermission("DeleteContainer", md, currContainerPropertiesFile))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                            {
                                _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "DeleteContainer", "denied"));
                            }

                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete AllowDeleteContainer operation not authorized per container permissions");
                            return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                        }

                        #endregion
                    }
                }
               
                #endregion

                #region Process-Replication

                if (!_Replication.ContainerDelete(md.CurrObj, _Topology.Replicas))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete negative response from replication, returning 500");
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to process replication.", null).ToJson(), true);
                }
                 
                _Bunker.ContainerDelete(md.CurrObj, recursive);

                #endregion

                #region Delete-Directory-and-Respond

                deleteSuccess = Common.DeleteDirectory(md.CurrObj.DiskPath, recursive);

                #endregion

                #region Respond

                if (deleteSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "ContainerDelete successfully deleted " + md.CurrObj.DiskPath);
                    return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete could not delete " + md.CurrObj.DiskPath);
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to delete container.", null).ToJson(), true);
                }

                #endregion

                #endregion
            }
            else
            {
                #region Remote-Owner

                switch (_Settings.Redirection.DeleteRedirectionMode)
                {
                    case "none":
                        #region none

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete object is destined for a different machine but DeleteRedirectionModee is none");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                    #endregion

                    case "proxy":
                        #region proxy

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerDelete proxying request to " + md.CurrObj.PrimaryUrlWithoutQs + " for container deletion");

                        proxyResponse = RestRequest.SendRequestSafe(
                            md.CurrObj.PrimaryUrlWithQs,
                            md.CurrHttpReq.ContentType,
                            md.CurrHttpReq.Method,
                            null, null, false,
                            Common.IsTrue(_Settings.Rest.AcceptInvalidCerts),
                            GetCustomHeaders(md.CurrHttpReq.Headers),
                            md.CurrHttpReq.Data);

                        if (proxyResponse == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete null response from proxy REST request to " + md.CurrObj.PrimaryUrlWithoutQs);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerDelete server response to proxy REST request: " + proxyResponse.StatusCode);
                        return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                    #endregion

                    case "redirect":
                        #region redirect

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerDelete redirecting request to " + md.CurrObj.PrimaryUrlWithoutQs + " using status " + _Settings.Redirection.DeleteRedirectHttpStatus + " for container deletion");
                        Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                        redirectHeader.Add("location", md.CurrObj.PrimaryUrlWithQs);
                        return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.DeleteRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.DeleteRedirectString, true);

                    #endregion

                    default:
                        #region unknown

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete unknown DeleteRedirectionModee in redirection settings: " + _Settings.Redirection.DeleteRedirectionMode);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                        #endregion
                }

                #endregion
            }

            #endregion
        }

        public HttpResponse Head(RequestMetadata md)
        {
            #region Variables

            string homeDirectory = "";
            RestResponse proxyResponse = new RestResponse();
            Dictionary<string, string> restHeaders = new Dictionary<string, string>();
            List<string> urls = new List<string>();
            string redirectUrl = "";
            string proxiedVal = "";
            bool proxied = false;
            string containerLogFile = "";
            string containerPropertiesFile = "";
            ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();

            #endregion

            #region Check-Permissions

            if (md.CurrPerm == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerHead null ApiKeyPermission object supplied");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json", null, true);
            }

            if (!Common.IsTrue(md.CurrPerm.AllowReadContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerHead AllowReadContainer operation not authorized per permissions");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json", null, true);
            }

            #endregion

            #region Get-Values-from-Querystring

            proxiedVal = md.CurrHttpReq.RetrieveHeaderValue("proxied");
            if (!String.IsNullOrEmpty(proxiedVal))
            {
                proxied = Common.IsTrue(proxiedVal);
            }

            #endregion

            #region Retrieve-User-Home-Directory

            homeDirectory = _UserMgr.GetHomeDirectory(md.CurrUser.Guid, _Settings);
            if (String.IsNullOrEmpty(homeDirectory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerHead unable to retrieve home directory for user GUID " + md.CurrUser.Guid);
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json", null, true);
            }

            #endregion

            #region Retrieve-Directory

            /*
                * Nodes that proxy requests will append ?proxied=true to the request URL
                * to notify the next recipient node to not proxy it further (otherwise
                * an infinite loop of REST requests will be generated)
                * 
                */

            if (proxied || (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId))
            {
                #region Local-Owner

                #region Check-Container-Permissions-and-Logging

                currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                if (currContainerPropertiesFile != null)
                {
                    if (currContainerPropertiesFile.Logging != null)
                    {
                        if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.ReadContainer))
                            {
                                #region Process-Logging

                                _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "ReadContainer", null));

                                #endregion
                            }
                        }
                    }

                    if (currContainerPropertiesFile.Permissions != null)
                    {
                        #region Evaluate-Permissions

                        if (!ContainerPermission.GetPermission("ReadContainer", md, currContainerPropertiesFile))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                            {
                                _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "ReadContainer", "denied"));
                            }

                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerHead AllowReadContainer operation not authorized per container permissions");
                            return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                        }

                        #endregion
                    }
                }

                #endregion

                #region Process

                if (!Common.DirectoryExists(homeDirectory)) Common.CreateDirectory(homeDirectory);

                DirInfo di = new DirInfo(_Settings, _UserMgr, _Logging);
                di = di.FromDirectory(md.CurrObj.DiskPath, md.CurrObj.UserGuid, 1, null, true);
                if (di == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerHead null info returned from " + md.CurrObj.DiskPath);
                    return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json", null, true);
                }

                return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);

                #endregion

                #endregion
            }
            else
            {
                #region Remote-Owner

                switch (_Settings.Redirection.ReadRedirectionMode)
                {
                    case "none":
                        #region none

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerHead object is stored on a different machine but ReadRedirectionMode is none");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                    #endregion

                    case "proxy":
                        #region proxy

                        if (_MaintenanceMgr.IsEnabled())
                        {
                            urls = Obj.BuildMaintReadUrls(true, md.CurrHttpReq, md.CurrObj, _Topology, _Logging);
                        }
                        else
                        {
                            urls = Obj.BuildReplicaUrls(true, md.CurrHttpReq, md.CurrObj, _Topology, _Logging);
                        }

                        if (urls == null || urls.Count < 1)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerHead unable to build replica URL list (null response)");
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to build proxy URL.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerHead proxying request to " + urls.Count + " URLs for user GUID " + md.CurrUser.Guid);

                        proxyResponse = FirstResponder.SendRequest(
                            _Settings,
                            _Logging,
                            md,
                            urls,
                            md.CurrHttpReq.ContentType,
                            md.CurrHttpReq.Method,
                            null, null, false,
                            GetCustomHeaders(md.CurrHttpReq.Headers),
                            md.CurrHttpReq.Data);

                        if (proxyResponse == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerHead null response from REST request to " + urls.Count + " URLs");
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerHead server response to proxy REST request: " + proxyResponse.StatusCode);
                        return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                    #endregion

                    case "redirect":
                        #region redirect

                        redirectUrl = Obj.BuildRedirectUrl(true, md.CurrHttpReq, md.CurrObj, _Topology, _Logging);
                        if (String.IsNullOrEmpty(redirectUrl))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerHead unable to generate redirect URL, returning 500");
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to build redirect URL.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerHead redirecting request using status " + _Settings.Redirection.ReadRedirectHttpStatus + " for user GUID " + md.CurrUser.Guid);
                        Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                        redirectHeader.Add("location", redirectUrl);
                        return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.ReadRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.ReadRedirectString, true);

                    #endregion

                    default:
                        #region unknown

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerHead unknown ReadRedirectionMode in redirection settings: " + _Settings.Redirection.ReadRedirectionMode);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                        #endregion
                }

                #endregion
            }

            #endregion
        }

        public HttpResponse Move(RequestMetadata md)
        {
            #region Variables

            MoveRequest req = new MoveRequest();
            RestResponse proxyResponse = new RestResponse();
            Dictionary<string, string> restHeaders = new Dictionary<string, string>();
            bool userGatewayMode = false;
            string containerLogFile = "";
            string containerPropertiesFile = "";
            ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();

            #endregion

            #region Check-Permissions

            if (md.CurrPerm == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove null ApiKeyPermission object supplied");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
            }

            if (!Common.IsTrue(md.CurrPerm.AllowWriteContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove AllowWriteContainer operation not authorized per permissions");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
            }

            #endregion

            #region Process-Owner

            if (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId)
            {
                #region Local-Owner

                #region Check-Container-Permissions-and-Logging

                currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                if (currContainerPropertiesFile != null)
                {
                    if (currContainerPropertiesFile.Logging != null)
                    {
                        if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.ReadContainer))
                            {
                                #region Process-Logging

                                _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteContainer-Move", null));

                                #endregion
                            }
                        }
                    }

                    if (currContainerPropertiesFile.Permissions != null)
                    {
                        #region Evaluate-Permissions

                        if (!ContainerPermission.GetPermission("WriteContainer", md, currContainerPropertiesFile))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                            {
                                _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteContainer-Move", "denied"));
                            }

                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove AllowWriteContainer operation not authorized per container permissions");
                            return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                        }

                        #endregion
                    }
                }

                #endregion

                #region Deserialize

                try
                {
                    req = Common.DeserializeJson<MoveRequest>(md.CurrHttpReq.Data);
                    if (req == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove null request after deserialization, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                    }
                }
                catch (Exception)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove unable to deserialize request body");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                }

                req.UserGuid = String.Copy(md.CurrUser.Guid);

                #endregion

                #region Validate-Request-Body

                if (req.FromContainer == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove null value supplied for FromContainer, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid value for FromContainer.", null).ToJson(), true);
                }

                if (req.ToContainer == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove null value supplied for ToContainer, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid value for ToContainer.", null).ToJson(), true);
                }

                if (String.IsNullOrEmpty(req.MoveFrom))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove null value supplied for MoveFrom, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid value for MoveFrom.", null).ToJson(), true);
                }

                if (String.IsNullOrEmpty(req.MoveTo))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove null value supplied for MoveTo, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid value for MoveTo.", null).ToJson(), true);
                }

                if (FsHelper.ContainsUnsafeFsChars(req))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove unsafe characters detected in request, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unsafe characters detected.", null).ToJson(), true);
                }

                req.UserGuid = md.CurrUser.Guid;

                #endregion

                #region Check-if-Original-Exists

                string diskPathOriginal = MoveRequest.BuildDiskPath(req, true, true, _UserMgr, _Settings, _Logging);
                if (String.IsNullOrEmpty(diskPathOriginal))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove unable to build disk path for original container");
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                }

                if (!Common.DirectoryExists(diskPathOriginal))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove original container does not exist: " + diskPathOriginal);
                    return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Source container does not exist.", null).ToJson(), true);
                }

                #endregion

                #region Check-if-Target-Parent-Exists

                string diskPathTargetParent = MoveRequest.BuildDiskPath(req, false, false, _UserMgr, _Settings, _Logging);
                if (String.IsNullOrEmpty(diskPathTargetParent))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove unable to build disk path for target container");
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                }

                if (!Common.DirectoryExists(diskPathTargetParent))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove target parent container does not exist: " + diskPathOriginal);
                    return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Target container does not exist.", null).ToJson(), true);
                }

                #endregion

                #region Check-if-Target-Child-Exists

                string diskPathTargetChild = MoveRequest.BuildDiskPath(req, false, true, _UserMgr, _Settings, _Logging);
                if (String.IsNullOrEmpty(diskPathTargetChild))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove unable to build disk path for target container");
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                }

                if (Common.DirectoryExists(diskPathTargetChild))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove target container already exists: " + diskPathOriginal);
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Container already exists.", null).ToJson(), true);
                }

                #endregion

                #region Set-Gateway-Mode

                userGatewayMode = md.CurrUser.GetGatewayMode(_Settings);

                #endregion

                #region Process-Replication

                if (!_Replication.ContainerMove(req))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove negative response from replication, returning 500");
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to process replication.", null).ToJson(), true);
                }

                _Bunker.ContainerMove(req);

                #endregion

                #region Move-Directory

                if (!Common.MoveDirectory(diskPathOriginal, diskPathTargetChild))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove unable to move container from " + diskPathOriginal + " to " + diskPathTargetChild);
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Container already exists.", null).ToJson(), true);
                }

                #endregion

                #region Perform-Background-Rewrite

                if (!userGatewayMode)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "ContainerMove spawning background task to rewrite objects with correct metadata");
                    Task.Run(() => RewriteTree(diskPathTargetChild));
                }

                #endregion

                return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);

                #endregion
            }
            else
            {
                #region Remote-Owner

                switch (_Settings.Redirection.WriteRedirectionMode)
                {
                    case "none":
                        #region none

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove object is destined for a different machine but WriteRedirectionMode is none");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                    #endregion

                    case "proxy":
                        #region proxy

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerMove proxying request to " + md.CurrObj.PrimaryUrlWithoutQs);

                        proxyResponse = RestRequest.SendRequestSafe(
                            md.CurrObj.PrimaryUrlWithQs,
                            md.CurrHttpReq.ContentType,
                            md.CurrHttpReq.Method,
                            null, null, false,
                            Common.IsTrue(_Settings.Rest.AcceptInvalidCerts),
                            GetCustomHeaders(md.CurrHttpReq.Headers),
                            md.CurrHttpReq.Data);

                        if (proxyResponse == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove null response from proxy REST request to " + md.CurrObj.PrimaryUrlWithoutQs);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable tocommunicate with the appropriate node for this request.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerMove server response to proxy REST request: " + proxyResponse.StatusCode);
                        return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                    #endregion

                    case "redirect":
                        #region redirect

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerMove redirecting request to " + md.CurrObj.PrimaryUrlWithoutQs + " using status " + _Settings.Redirection.WriteRedirectHttpStatus);
                        Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                        redirectHeader.Add("location", md.CurrObj.PrimaryUrlWithQs);
                        return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.WriteRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.WriteRedirectString, true);

                    #endregion

                    default:
                        #region unknown

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerMove unknown WriteRedirectionMode in redirection settings: " + _Settings.Redirection.WriteRedirectionMode);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                        #endregion
                }

                #endregion
            }

            #endregion
        }

        public HttpResponse Read(RequestMetadata md)
        {
            #region Variables

            string homeDirectory = "";
            RestResponse proxyResponse = new RestResponse();
            Dictionary<string, string> restHeaders = new Dictionary<string, string>();
            List<string> urlList = new List<string>();
            string redirectUrl = "";

            string metadataVal = "";
            bool metadataOnly = false;
            string proxiedVal = "";
            bool proxied = false;
            string recursiveVal = "";
            bool recursive = false;
            string statsVal = "";
            bool stats = false;
            string maxResultsStr = "";
            int maxResults = 0;
            string walkVal = "";
            bool walk = false;
            string debugVal = "";
            bool debug = false;
            string propertiesVal = "";
            bool properties = false;

            string containerLogFile = "";
            string containerPropertiesFile = "";
            ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();

            #endregion

            #region Check-Permissions

            if (md.CurrPerm == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead null ApiKeyPermission object supplied");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
            }

            if (!Common.IsTrue(md.CurrPerm.AllowReadContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead AllowReadContainer operation not authorized per permissions");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
            }

            #endregion

            #region Get-Values-from-Querystring

            maxResultsStr = md.CurrHttpReq.RetrieveHeaderValue("max_results");
            if (!String.IsNullOrEmpty(maxResultsStr))
            {
                if (!Int32.TryParse(maxResultsStr, out maxResults))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead invalid value for max_results in querystring: " + maxResultsStr);
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid value for max_results.", null).ToJson(), true);
                }
            }

            metadataVal = md.CurrHttpReq.RetrieveHeaderValue("metadata");
            if (!String.IsNullOrEmpty(metadataVal))
            {
                metadataOnly = Common.IsTrue(metadataVal);
            }

            proxiedVal = md.CurrHttpReq.RetrieveHeaderValue("proxied");
            if (!String.IsNullOrEmpty(proxiedVal))
            {
                proxied = Common.IsTrue(proxiedVal);
            }

            statsVal = md.CurrHttpReq.RetrieveHeaderValue("stats");
            if (!String.IsNullOrEmpty(statsVal))
            {
                stats = Common.IsTrue(statsVal);
            }

            recursiveVal = md.CurrHttpReq.RetrieveHeaderValue("recursive");
            if (!String.IsNullOrEmpty(recursiveVal))
            {
                recursive = Common.IsTrue(recursiveVal);
            }

            walkVal = md.CurrHttpReq.RetrieveHeaderValue("walk");
            if (!String.IsNullOrEmpty(walkVal))
            {
                walk = Common.IsTrue(walkVal);
            }

            debugVal = md.CurrHttpReq.RetrieveHeaderValue("debug");
            if (!String.IsNullOrEmpty(debugVal))
            {
                debug = Common.IsTrue(debugVal);
            }

            propertiesVal = md.CurrHttpReq.RetrieveHeaderValue("properties");
            if (!String.IsNullOrEmpty(propertiesVal))
            {
                properties = Common.IsTrue(propertiesVal);
            }

            #endregion

            #region Retrieve-User-Home-Directory

            homeDirectory = _UserMgr.GetHomeDirectory(md.CurrUser.Guid, _Settings);
            if (String.IsNullOrEmpty(homeDirectory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead unable to retrieve home directory for user GUID " + md.CurrUser.Guid);
                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to find home directory for user.", null).ToJson(), true);
            }

            #endregion

            #region Retrieve-Directory

            /*
             * Nodes that proxy requests will append ?proxied=true to the request URL
             * to notify the next recipient node to not proxy it further (otherwise
             * an infinite loop of REST requests will be generated)
             * 
             */

            if (proxied || (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId))
            {
                #region Local-Owner

                #region Check-Container-Permissions-and-Logging

                currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                if (currContainerPropertiesFile != null)
                {
                    if (currContainerPropertiesFile.Logging != null)
                    {
                        if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.ReadContainer))
                            {
                                #region Process-Logging

                                _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "ReadContainer", null));

                                #endregion
                            }
                        }
                    }

                    if (currContainerPropertiesFile.Permissions != null)
                    {
                        #region Evaluate-Permissions

                        if (!ContainerPermission.GetPermission("ReadContainer", md, currContainerPropertiesFile))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                            {
                                _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "ReadContainer", "denied"));
                            }

                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead AllowReadContainer operation not authorized per container permissions");
                            return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                        }

                        #endregion
                    }
                }

                #endregion

                #region Create-Directory-if-Needed

                if (!Common.DirectoryExists(homeDirectory)) Common.CreateDirectory(homeDirectory);

                #endregion

                #region Gather-Directory-Info

                DirInfo di = new DirInfo(_Settings, _UserMgr, _Logging);
                di = di.FromDirectory(md.CurrObj.DiskPath, md.CurrObj.UserGuid, maxResults, null, metadataOnly);
                if (di == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead null info returned from " + md.CurrObj.DiskPath);
                    return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Container does not exist.", null).ToJson(), true);
                }

                di.UserGuid = md.CurrObj.UserGuid;
                di.Url = md.CurrObj.PrimaryUrlWithoutQs;
                di.ContainerPath = md.CurrObj.ContainerPath;

                #endregion

                #region Respond

                if (stats)
                {
                    #region stats

                    long bytes = 0;
                    int files = 0;
                    int subdirs = 0;
                    DirectoryInfo dirinfo = new DirectoryInfo(md.CurrObj.DiskPath);

                    if (Common.DirectoryStatistics(dirinfo, recursive, out bytes, out files, out subdirs))
                    {
                        #region stats-success

                        Dictionary<string, object> ret = new Dictionary<string, object>();
                        ret.Add("UserGuid", di.UserGuid);
                        ret.Add("Url", di.Url);
                        if (debug) ret.Add("DiskPath", md.CurrObj.DiskPath);
                        if (debug) ret.Add("PrimaryUrl", md.CurrObj.PrimaryUrlWithoutQs);
                        ret.Add("Recursive", recursive);
                        ret.Add("Bytes", bytes);
                        ret.Add("Objects", files);
                        ret.Add("Containers", subdirs);

                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(ret), true);

                        #endregion
                    }
                    else
                    {
                        #region stats-fail

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead unable to execute Common.DirectoryStatistics on " + md.CurrObj.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(1, 500, null, null).ToJson(), true);

                        #endregion
                    }

                    #endregion
                }
                else if (walk)
                {
                    #region walk

                    List<string> containers = new List<string>();
                    List<string> objects = new List<string>();
                    long bytes = 0;

                    if (Common.WalkDirectory(
                        _Settings.Environment,
                        0,
                        md.CurrObj.DiskPath,
                        true,
                        out containers,
                        out objects,
                        out bytes,
                        recursive))
                    {
                        #region walk-success

                        List<string> containersUpdated = new List<string>();
                        List<string> objectsUpdated = new List<string>();

                        foreach (string currContainer in containers)
                        {
                            string tempContainer = String.Copy(currContainer);
                            tempContainer = tempContainer.Replace(md.CurrObj.DiskPath, "");
                            tempContainer = tempContainer.Replace("\\", "/");
                            tempContainer = tempContainer.Replace("//", "/");
                            containersUpdated.Add(tempContainer);
                        }

                        foreach (string currObject in objects)
                        {
                            string tempObject = String.Copy(currObject);
                            tempObject = tempObject.Replace(md.CurrObj.DiskPath, "");
                            tempObject = tempObject.Replace("\\", "/");
                            tempObject = tempObject.Replace("//", "/");
                            while (tempObject.StartsWith("/")) tempObject = tempObject.Substring(1, tempObject.Length - 1);
                            objectsUpdated.Add(tempObject);
                        }

                        Dictionary<string, object> ret = new Dictionary<string, object>();
                        ret.Add("UserGuid", di.UserGuid);
                        ret.Add("Url", di.Url);
                        if (debug) ret.Add("DiskPath", md.CurrObj.DiskPath);
                        if (debug) ret.Add("PrimaryUrl", md.CurrObj.PrimaryUrlWithoutQs);
                        ret.Add("Recursive", recursive);
                        ret.Add("Bytes", bytes);
                        ret.Add("Objects", objects.Count);
                        ret.Add("Containers", containers.Count);
                        ret.Add("ObjectList", objectsUpdated);
                        ret.Add("ContainerList", containersUpdated);

                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(ret), true);

                        #endregion
                    }
                    else
                    {
                        #region walk-fail

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead unable to execute walk on " + md.CurrObj.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(1, 500, null, null).ToJson(), true);

                        #endregion
                    }

                    #endregion
                }
                else
                {
                    #region send-dir-info

                    return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(di), true);

                    #endregion
                }

                #endregion

                #endregion
            }
            else
            {
                #region Remote-Owner

                switch (_Settings.Redirection.ReadRedirectionMode)
                {
                    case "none":
                        #region none

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead object is stored on a different machine but ReadRedirectionMode is none");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                    #endregion

                    case "proxy":
                        #region proxy

                        if (_MaintenanceMgr.IsEnabled())
                        {
                            urlList = Obj.BuildMaintReadUrls(true, md.CurrHttpReq, md.CurrObj, _Topology, _Logging);
                        }
                        else
                        {
                            urlList = Obj.BuildReplicaUrls(true, md.CurrHttpReq, md.CurrObj, _Topology, _Logging);
                        }

                        if (urlList == null || urlList.Count < 1)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead unable to build replica URL list (null response)");
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to build proxy URL.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerRead proxying request to " + urlList.Count + " URLs for user GUID " + md.CurrUser.Guid);

                        proxyResponse = FirstResponder.SendRequest(
                            _Settings,
                            _Logging,
                            md,
                            urlList,
                            md.CurrHttpReq.ContentType,
                            md.CurrHttpReq.Method,
                            null, null, false,
                            GetCustomHeaders(md.CurrHttpReq.Headers),
                            md.CurrHttpReq.Data);

                        if (proxyResponse == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead null response from REST request to " + urlList.Count + " URLs");
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerRead server response to proxy REST request: " + proxyResponse.StatusCode);
                        return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                    #endregion

                    case "redirect":
                        #region redirect

                        redirectUrl = Obj.BuildRedirectUrl(true, md.CurrHttpReq, md.CurrObj, _Topology, _Logging);
                        if (String.IsNullOrEmpty(redirectUrl))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead unable to generate redirect_url, returning 500");
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to build redirect URL.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerRead redirecting request using status " + _Settings.Redirection.ReadRedirectHttpStatus + " for user GUID " + md.CurrUser.Guid);
                        Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                        redirectHeader.Add("location", redirectUrl);
                        return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.ReadRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.ReadRedirectString, true);

                    #endregion

                    default:
                        #region unknown

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerRead unknown ReadRedirectionMode in redirection settings: " + _Settings.Redirection.ReadRedirectionMode);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                        #endregion
                }

                #endregion
            }

            #endregion
        }

        public HttpResponse Rename(RequestMetadata md)
        {
            #region Variables

            RenameRequest req = new RenameRequest();
            RestResponse proxyResponse = new RestResponse();
            Dictionary<string, string> restHeaders = new Dictionary<string, string>();
            bool userGatewayMode = false;
            string containerLogFile = "";
            string containerPropertiesFile = "";
            ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();

            #endregion

            #region Check-Permissions

            if (md.CurrPerm == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename null ApiKeyPermission object supplied");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
            }

            if (!Common.IsTrue(md.CurrPerm.AllowWriteContainer))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename AllowWriteContainer operation not authorized per permissions");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
            }

            #endregion

            #region Process-Owner

            if (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId)
            {
                #region Local-Owner

                #region Check-Container-Permissions-and-Logging

                currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                if (currContainerPropertiesFile != null)
                {
                    if (currContainerPropertiesFile.Logging != null)
                    {
                        if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.ReadContainer))
                            {
                                #region Process-Logging

                                _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteContainer-Rename", null));

                                #endregion
                            }
                        }
                    }

                    if (currContainerPropertiesFile.Permissions != null)
                    {
                        #region Evaluate-Permissions

                        if (!ContainerPermission.GetPermission("WriteContainer", md, currContainerPropertiesFile))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                            {
                                _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteContainer-Rename", "denied"));
                            }

                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename AllowWriteContainer operation not authorized per container permissions");
                            return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                        }

                        #endregion
                    }
                }

                #endregion

                #region Deserialize

                try
                {
                    req = Common.DeserializeJson<RenameRequest>(md.CurrHttpReq.Data);
                    if (req == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename null request after deserialization, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                    }
                }
                catch (Exception)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename unable to deserialize request body");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                }

                req.UserGuid = String.Copy(md.CurrUser.Guid);

                #endregion

                #region Validate-Request-Body

                if (req.ContainerPath == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename null value supplied for ContainerPath, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid value for ContainerPath.", null).ToJson(), true);
                }

                if (String.IsNullOrEmpty(req.RenameFrom))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename null value supplied for RenameFrom, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid value for RenameFrom.", null).ToJson(), true);
                }

                if (String.IsNullOrEmpty(req.RenameTo))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename null value supplied for RenameTo, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid value for RenameTo.", null).ToJson(), true);

                }

                if (FsHelper.ContainsUnsafeFsChars(req))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename unsafe characters detected in request, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unsafe characters detected.", null).ToJson(), true);

                }

                req.UserGuid = md.CurrUser.Guid;

                #endregion

                #region Check-if-Original-Exists

                string diskPathOriginal = RenameRequest.BuildDiskPath(req, true, _UserMgr, _Settings, _Logging);
                if (String.IsNullOrEmpty(diskPathOriginal))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename unable to build disk path for original container");
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                }

                if (!Common.DirectoryExists(diskPathOriginal))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename original container does not exist: " + diskPathOriginal);
                    return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Container does not exist.", null).ToJson(), true);
                }

                #endregion

                #region Check-if-Target-Exists

                string diskPathTarget = RenameRequest.BuildDiskPath(req, false, _UserMgr, _Settings, _Logging);
                if (String.IsNullOrEmpty(diskPathTarget))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename unable to build disk path for target container");
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                }

                if (Common.DirectoryExists(diskPathTarget))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename target container already exists: " + diskPathOriginal);
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Container already exists.", null).ToJson(), true);

                }

                #endregion

                #region Set-Gateway-Mode

                userGatewayMode = md.CurrUser.GetGatewayMode(_Settings);

                #endregion

                #region Process-Replication

                if (!_Replication.ContainerRename(req))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename negative response from replication, returning 500");
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to process replication.", null).ToJson(), true);
                }

                _Bunker.ContainerRename(req);

                #endregion

                #region Rename-Directory

                if (!Common.RenameDirectory(diskPathOriginal, diskPathTarget))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename unable to rename container from " + diskPathOriginal + " to " + diskPathTarget);
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Container already exists.", null).ToJson(), true);
                }

                #endregion

                #region Perform-Background-Rewrite

                if (!userGatewayMode)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "ContainerRename spawning background task to rewrite objects with correct metadata");
                    Task.Run(() => RewriteTree(diskPathTarget));
                }

                #endregion

                return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);

                #endregion
            }
            else
            {
                #region Remote-Owner

                switch (_Settings.Redirection.WriteRedirectionMode)
                {
                    case "none":
                        #region none

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename object is destined for a different machine but WriteRedirectionMode is none");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                    #endregion

                    case "proxy":
                        #region proxy

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerRename proxying request to " + md.CurrObj.PrimaryUrlWithoutQs);

                        proxyResponse = RestRequest.SendRequestSafe(
                            md.CurrObj.PrimaryUrlWithQs,
                            md.CurrHttpReq.ContentType,
                            md.CurrHttpReq.Method,
                            null, null, false,
                            Common.IsTrue(_Settings.Rest.AcceptInvalidCerts),
                            GetCustomHeaders(md.CurrHttpReq.Headers),
                            md.CurrHttpReq.Data);

                        if (proxyResponse == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename null response from proxy REST request to " + md.CurrObj.PrimaryUrlWithoutQs);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerRename server response to proxy REST request: " + proxyResponse.StatusCode);
                        return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                    #endregion

                    case "redirect":
                        #region redirect

                        _Logging.Log(LoggingModule.Severity.Debug, "ContainerRename redirecting request to " + md.CurrObj.PrimaryUrlWithoutQs + " using status " + _Settings.Redirection.WriteRedirectHttpStatus);
                        Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                        return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.WriteRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.WriteRedirectString, true);

                    #endregion

                    default:
                        #region unknown

                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerRename unknown WriteRedirectionMode in redirection settings: " + _Settings.Redirection.WriteRedirectionMode);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                        #endregion
                }

                #endregion
            }

            #endregion
        }

        public HttpResponse Write(RequestMetadata md)
        {
            try
            {
                #region try

                #region Variables

                string homeDirectory = "";
                RestResponse proxyResponse = new RestResponse();
                Dictionary<string, string> restHeaders = new Dictionary<string, string>();
                bool localSuccess = false;
                List<Node> successfulReplicas = new List<Node>();
                string containerLogFile = "";
                string containerPropertiesFile = "";
                ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();

                #endregion

                #region Check-Permissions

                if (md.CurrPerm == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite null ApiKeyPermission object supplied");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                }

                if (!Common.IsTrue(md.CurrPerm.AllowWriteContainer))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite AllowWriteContainer operation not authorized per permissions");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                }

                #endregion

                #region Retrieve-User-Home-Directory

                homeDirectory = _UserMgr.GetHomeDirectory(md.CurrUser.Guid, _Settings);
                if (String.IsNullOrEmpty(homeDirectory))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite unable to retrieve home directory for user GUID " + md.CurrUser.Guid);
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to find home directory for user.", null).ToJson(), true);
                }

                #endregion

                #region Process-Owner

                if (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId)
                {
                    #region Local-Owner

                    #region Check-Container-Permissions-and-Logging

                    currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                    if (currContainerPropertiesFile != null)
                    {
                        if (currContainerPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.ReadContainer))
                                {
                                    #region Process-Logging

                                    _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteContainer", null));

                                    #endregion
                                }
                            }
                        }

                        if (currContainerPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ContainerPermission.GetPermission("WriteContainer", md, currContainerPropertiesFile))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                                {
                                    _LoggerMgr.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteContainer", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite AllowWriteContainer operation not authorized per container permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Create-User-Folder-if-Needed

                    if (!Common.DirectoryExists(homeDirectory)) Common.CreateDirectory(homeDirectory);

                    #endregion

                    #region Process-Replication

                    if (!_Replication.ContainerWrite(md.CurrObj, out successfulReplicas))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite negative response from replication, returning 500");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to process replication.", null).ToJson(), true);
                    }

                    _Bunker.ContainerWrite(md.CurrObj);

                    #endregion

                    #region Create-Directory-Locally

                    localSuccess = Common.CreateDirectory(md.CurrObj.DiskPath);
                    if (!localSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite unable to write container to " + md.CurrObj.DiskPath);
                        Task.Run(() =>
                        {
                            _Replication.ContainerDelete(md.CurrObj, _Topology.Replicas);
                        });

                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to store container.", null).ToJson(), true);
                    }

                    _Logging.Log(LoggingModule.Severity.Debug, "ContainerWrite successfully created container " + md.CurrObj.DiskPath);

                    #endregion

                    return new HttpResponse(md.CurrHttpReq, true, 200, null, "text/plain", md.CurrObj.PrimaryUrlWithoutQs, true);

                    #endregion
                }
                else
                {
                    #region Remote-Owner

                    switch (_Settings.Redirection.WriteRedirectionMode)
                    {
                        case "none":
                            #region none

                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite object is destined for a different machine but WriteRedirectionMode is none");
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                        #endregion

                        case "proxy":
                            #region proxy

                            _Logging.Log(LoggingModule.Severity.Debug, "ContainerWrite proxying request to " + md.CurrObj.PrimaryUrlWithoutQs);

                            proxyResponse = RestRequest.SendRequestSafe(
                                md.CurrObj.PrimaryUrlWithQs,
                                md.CurrHttpReq.ContentType,
                                md.CurrHttpReq.Method,
                                null, null, false,
                                Common.IsTrue(_Settings.Rest.AcceptInvalidCerts),
                                GetCustomHeaders(md.CurrHttpReq.Headers),
                                md.CurrHttpReq.Data);

                            if (proxyResponse == null)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite null response from proxy REST request to " + md.CurrObj.PrimaryUrlWithoutQs);
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ContainerWrite server response to proxy REST request: " + proxyResponse.StatusCode);
                            return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                        #endregion

                        case "redirect":
                            #region redirect

                            _Logging.Log(LoggingModule.Severity.Debug, "ContainerWrite redirecting request to " + md.CurrObj.PrimaryUrlWithoutQs + " using status " + _Settings.Redirection.WriteRedirectHttpStatus);
                            Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                            redirectHeader.Add("location", md.CurrObj.PrimaryUrlWithQs);
                            return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.WriteRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.WriteRedirectString, true);

                        #endregion

                        default:
                            #region unknown

                            _Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite unknown WriteRedirectionMode in redirection settings: " + _Settings.Redirection.WriteRedirectionMode);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                            #endregion
                    }

                    #endregion
                }

                #endregion

                #endregion
            }
            catch (IOException ioeOuter)
            {
                #region disk-full

                if (
                    ((ioeOuter.HResult & 0xFFFF) == 0x27)
                    || ((ioeOuter.HResult & 0xFFFF) == 0x70)
                   )
                {
                    _Logging.Log(LoggingModule.Severity.Alert, "ContainerWrite disk full detected during write operation for " + md.CurrUser.Guid + " " + md.CurrObj.Key);
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(1, 500, "Disk is full.", null).ToJson(), true);
                }

                #endregion

                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json", null, true);
            }
        }

        #endregion

        #region Private-Methods

        private Dictionary<string, string> GetCustomHeaders(Dictionary<string, string> headers)
        {
            if (headers == null) return null;
            if (headers.Count == 0) return null;
            Dictionary<string, string> ret = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string> curr in headers)
            {
                if (String.Compare(curr.Key, _Settings.Server.HeaderApiKey) == 0
                    || String.Compare(curr.Key, _Settings.Server.HeaderEmail) == 0
                    || String.Compare(curr.Key, _Settings.Server.HeaderPassword) == 0
                    || String.Compare(curr.Key, _Settings.Server.HeaderToken) == 0
                    || String.Compare(curr.Key, _Settings.Server.HeaderVersion) == 0)
                {
                    if (ret.ContainsKey(curr.Key))
                    {
                        string tempVal = ret[curr.Key];
                        tempVal += "," + curr.Value;
                        ret.Remove(curr.Key);
                        ret.Add(curr.Key, tempVal);
                    }
                    else
                    {
                        ret.Add(curr.Key, curr.Value);
                    }
                }
            }

            return ret;
        }

        private bool RewriteTree(string root)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(root))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RewriteTree null root directory supplied");
                return false;
            }

            #endregion

            #region Variables

            List<string> dirlist = new List<string>();
            List<string> filelist = new List<string>();
            long byteCount = 0;

            #endregion

            #region Get-Full-File-List

            if (!Common.WalkDirectory(_Settings.Environment, 0, root, true, out dirlist, out filelist, out byteCount, true))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RewriteTree unable to walk directory for " + root);
                return false;
            }

            #endregion

            #region Process-Each-File

            foreach (string currFile in filelist)
            {
                if (!RewriteObject(currFile))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "RewriteTree unable to rewrite file " + currFile);
                }
            }

            #endregion

            return true;
        }

        private bool RewriteObject(string filename)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(filename))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RewriteObject null filename supplied");
                return false;
            }

            #endregion

            #region Variables

            Obj currObj = new Obj();
            List<string> containers = new List<string>();
            string random = "";
            bool writeSuccess = false;

            #endregion

            #region Retrieve-Object

            currObj = Obj.BuildObjFromDisk(filename, _UserMgr, _Settings, _Topology, _Node, _Logging);
            if (currObj == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RewriteObject unable to build disk obj from file " + filename);
                return false;
            }

            #endregion

            #region Generate-Random-String

            random = Common.RandomString(8);

            #endregion

            #region Rename-Original

            if (!Common.RenameFile(filename, filename + "." + random))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RewriteObject unable to rename " + filename + " to temporary filename " + filename + "." + random);
                return false;
            }

            #endregion

            #region Delete-File

            if (!Common.DeleteFile(filename))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RewriteObject unable to delete file " + filename);
                return false;
            }

            #endregion

            #region Rewrite-File

            if (!Common.IsTrue(currObj.GatewayMode))
            {
                writeSuccess = Common.WriteFile(filename, Common.SerializeJson(currObj), false);
                if (!writeSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "RewriteObject unable to write object to " + filename);
                    return false;
                }
            }
            else
            {
                writeSuccess = Common.WriteFile(filename, currObj.Value);
                if (!writeSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "RewriteObject unable to write raw bytes to " + filename);
                    return false;
                }
            }
                
            #endregion

            #region Delete-Temporary-File-and-Return

            if (!Common.DeleteFile(filename + "." + random))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RewriteObject " + filename + " was successfully rerwritten but temporary file " + filename + "." + random + " was unable to be deleted");
                return true;
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Debug, "RewriteObject successfully rewrote object " + filename);
                return true;
            }

            #endregion
        }

        #endregion
    }
}
