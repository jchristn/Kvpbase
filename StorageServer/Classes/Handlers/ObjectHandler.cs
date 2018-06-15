using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using RestWrapper;
using WatsonWebserver;

namespace Kvpbase
{
    public class ObjectHandler
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
        private UrlLockManager _LockMgr;
        private MaintenanceManager _MaintenanceMgr;
        private EncryptionModule _Encryption;
        private LoggerManager _Logger;
        private BunkerHandler _Bunker;
        private ReplicationHandler _Replication;
        private ObjManager _ObjMgr;

        #endregion

        #region Constructors-and-Factories

        public ObjectHandler(
            Settings settings,
            Events logging,
            MessageManager messages,
            Topology topology,
            Node node,
            UserManager users,
            UrlLockManager locks,
            MaintenanceManager maintenance,
            EncryptionModule encryption,
            LoggerManager logger,
            BunkerHandler bunker,
            ReplicationHandler replication,
            ObjManager obj)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (messages == null) throw new ArgumentNullException(nameof(messages));
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (users == null) throw new ArgumentNullException(nameof(users));
            if (locks == null) throw new ArgumentNullException(nameof(locks));
            if (maintenance == null) throw new ArgumentNullException(nameof(maintenance));
            if (encryption == null) throw new ArgumentNullException(nameof(encryption));
            if (logger == null) throw new ArgumentNullException(nameof(logger));
            if (bunker == null) throw new ArgumentNullException(nameof(bunker));
            if (replication == null) throw new ArgumentNullException(nameof(replication));
            if (obj == null) throw new ArgumentNullException(nameof(obj));

            _Settings = settings;
            _Logging = logging;
            _MessageMgr = messages;
            _Topology = topology;
            _Node = node;
            _UserMgr = users;
            _LockMgr = locks;
            _MaintenanceMgr = maintenance;
            _Encryption = encryption;
            _Logger = logger;
            _Bunker = bunker;
            _Replication = replication;
            _ObjMgr = obj;
        }

        #endregion

        #region Public-Methods

        public HttpResponse Delete(RequestMetadata md)
        {
            DateTime startTime = DateTime.Now;
            bool locked = false;

            try
            {
                #region try

                #region Variables

                RestResponse proxyResponse = new RestResponse();
                Dictionary<string, string> restHeaders = new Dictionary<string, string>();
                ObjInfo currObjInfo = new ObjInfo();
                bool deleteSuccess = false;
                string containerLogFile = "";
                string containerPropertiesFile = "";
                ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();
                string objectLogFile = "";
                string objectPropertiesFile = "";
                ObjectPropertiesFile currObjectPropertiesFile = new ObjectPropertiesFile();

                #endregion

                #region Check-Permissions

                if (md.CurrPerm == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete null ApiKeyPermission object supplied");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                }

                if (!Common.IsTrue(md.CurrPerm.AllowDeleteObject))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete AllowDeleteObject operation not authorized per permissions");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                }

                #endregion

                #region Process-Owner

                if (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId)
                {
                    #region Local-Owner

                    #region Add-Lock

                    locked = _LockMgr.LockUrl(md);
                    if (!locked)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete " + md.CurrObj.DiskPath + " is unable to be locked");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(9, 423, "Resource in use.", null).ToJson(), true);
                    }

                    #endregion

                    #region Check-Container-Permissions-and-Logging

                    currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                    if (currContainerPropertiesFile != null)
                    {
                        if (currContainerPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.DeleteObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "DeleteObject", null));

                                    #endregion
                                }
                            }
                        }

                        if (currContainerPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ContainerPermission.GetPermission("DeleteObject", md, currContainerPropertiesFile))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "DeleteObject", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete AllowDeleteObject operation not authorized per container permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Check-Object-Permissions-and-Logging

                    currObjectPropertiesFile = ObjectPropertiesFile.FromObject(md.CurrObj, out objectLogFile, out objectPropertiesFile);
                    if (currObjectPropertiesFile != null)
                    {
                        if (currObjectPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.DeleteObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectDelete", null));

                                    #endregion
                                }
                            }
                        }

                        if (currObjectPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ObjectPermission.GetPermission("ObjectDelete", md, currObjectPropertiesFile))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectDelete", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete AllowDeleteObject operation not authorized per object permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Retrieve-Object-Metadata

                    currObjInfo = ObjInfo.FromFile(md.CurrObj.DiskPath);
                    if (currObjInfo == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete null file info returned for " + md.CurrObj.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                            new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                    }

                    #endregion

                    #region Process-Replication

                    if (!_Replication.ObjectDelete(md.CurrObj, md.CurrObj.Replicas))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete negative response from replication, returning 500");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to process replication.", null).ToJson(), true);
                    }

                    _Bunker.ObjectDelete(md.CurrObj);

                    #endregion

                    #region Delete-and-Respond

                    deleteSuccess = Common.DeleteFile(md.CurrObj.DiskPath);
                    if (deleteSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "ObjectDelete successfully deleted " + md.CurrObj.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete could not delete " + md.CurrUser.Guid + " " + md.CurrObj.Key);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to delete object.", null).ToJson(), true);
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

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete object is destined for a different machine but DeleteRedirectionModee is none");
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                        #endregion

                        case "proxy":
                            #region proxy

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectDelete proxying request to " + md.CurrObj.PrimaryUrlWithoutQs + " for object key " + md.CurrObj.Key);

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
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete null response from proxy REST request to " + md.CurrObj.PrimaryUrlWithoutQs);
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectDelete server response to proxy REST request: " + proxyResponse.StatusCode);
                            return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                        #endregion

                        case "redirect":
                            #region redirect

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectDelete redirecting request to " + md.CurrObj.PrimaryUrlWithoutQs +
                                " using status " + _Settings.Redirection.DeleteRedirectHttpStatus +
                                " for object key " + md.CurrObj.Key);
                            Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                            redirectHeader.Add("location", md.CurrObj.PrimaryUrlWithQs);
                            return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.DeleteRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.DeleteRedirectString, true);

                        #endregion

                        default:
                            #region unknown

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete unknown DeleteRedirectionModee in redirection settings: " + _Settings.Redirection.DeleteRedirectionMode);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                            #endregion
                    }

                    #endregion
                }

                #endregion

                #endregion
            }
            finally
            {
                #region finally

                #region unlock

                if (locked)
                {
                    if (!_LockMgr.UnlockUrl(md))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete unable to unlock " + md.CurrObj.DiskPath);
                    }
                }

                #endregion

                #endregion
            }
        }

        public HttpResponse Head(RequestMetadata md)
        {
            DateTime startTime = DateTime.Now;
            bool locked = false;

            try
            {
                #region try

                #region Variables

                RestResponse proxyResponse = new RestResponse();
                Dictionary<string, string> restHeaders = new Dictionary<string, string>();
                List<string> urls = new List<string>();
                string redirectUrls = "";
                string proxiedVal = "";
                bool proxied = false;
                ObjInfo currObjInfo = new ObjInfo();
                string containerLogFile = "";
                string containerPropertiesFile = "";
                ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();
                string objectLogFile = "";
                string objectPropertiesFile = "";
                ObjectPropertiesFile currObjectPropertiesFile = new ObjectPropertiesFile();

                #endregion

                #region Check-Permissions

                if (md.CurrPerm == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead null ApiKeyPermission object supplied");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, null, null).ToJson(), true);
                }

                if (!Common.IsTrue(md.CurrPerm.AllowReadObject))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead AllowReadObject operation not authorized per permissions");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, null, null).ToJson(), true);
                }

                #endregion

                #region Check-for-Key-in-URL

                if (String.IsNullOrEmpty(md.CurrObj.Key))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead unable to find object key in URL");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(2, 400, null, null).ToJson(), true);

                }

                #endregion

                #region Get-Values-from-Querystring

                proxiedVal = md.CurrHttpReq.RetrieveHeaderValue("proxied");
                if (!String.IsNullOrEmpty(proxiedVal))
                {
                    proxied = Common.IsTrue(proxiedVal);
                }

                #endregion

                #region Retrieve-Specific-Object

                /*
                 * Nodes that proxy requests will append ?proxied=true to the request URL
                 * to notify the next recipient node to not proxy it further (otherwise
                 * an infinite loop of REST requests will be generated)
                 * 
                 */

                if (proxied || (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId))
                {
                    #region Local-Owner

                    #region Add-Lock

                    locked = _LockMgr.AddReadResource(md.CurrObj.DiskPath);
                    if (!locked)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead " + md.CurrObj.DiskPath + " is unable to be locked");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(9, 423, "Resource in use.", null).ToJson(), true);
                    }

                    #endregion

                    #region Check-Container-Permissions-and-Logging

                    currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                    if (currContainerPropertiesFile != null)
                    {
                        if (currContainerPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.ReadObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "ReadObject", null));

                                    #endregion
                                }
                            }
                        }

                        if (currContainerPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ContainerPermission.GetPermission("ReadObject", md, currContainerPropertiesFile))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "ReadObject", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead AllowReadObject operation not authorized per container permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Check-Object-Permissions-and-Logging

                    currObjectPropertiesFile = ObjectPropertiesFile.FromObject(md.CurrObj, out objectLogFile, out objectPropertiesFile);
                    if (currObjectPropertiesFile != null)
                    {
                        if (currObjectPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.ReadObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectHead", null));

                                    #endregion
                                }
                            }
                        }

                        if (currObjectPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ObjectPermission.GetPermission("ReadObject", md, currObjectPropertiesFile))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectHead", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead AllowReadObject operation not authorized per object permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Process

                    currObjInfo = ObjInfo.FromFile(md.CurrObj.DiskPath);
                    if (currObjInfo == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead null file info returned for " + md.CurrObj.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                            new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
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

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead object is stored on a different machine but ReadRedirectionMode is none");
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                        #endregion

                        case "proxy":
                            #region proxy

                            if (_MaintenanceMgr.IsEnabled())
                            {
                                urls = _ObjMgr.MaintenanceUrls(true, md.CurrHttpReq, md.CurrObj);
                            }
                            else
                            {
                                urls = _ObjMgr.ReplicaUrls(true, md.CurrHttpReq, md.CurrObj);
                            }

                            if (urls == null || urls.Count < 1)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead unable to build replica URL list (null response)");
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to build proxy URL.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectHead proxying request to " + urls.Count + " URLs for user GUID " + md.CurrUser.Guid);

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
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead null response from proxy REST request to " + urls.Count + " URLs");
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectHead server response to proxy REST request: " + proxyResponse.StatusCode);
                            return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                        #endregion

                        case "redirect":
                            #region redirect

                            redirectUrls = _ObjMgr.RedirectUrl(true, md.CurrHttpReq, md.CurrObj);
                            if (String.IsNullOrEmpty(redirectUrls))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead unable to generate redirect URL, returning 500");
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to build redirect URL.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectHead redirecting request using status " + _Settings.Redirection.ReadRedirectHttpStatus + " for user GUID " + md.CurrUser.Guid);
                            Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                            redirectHeader.Add("location", redirectUrls);
                            return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.ReadRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.ReadRedirectString, true);

                        #endregion

                        default:
                            #region unknown

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead unknown ReadRedirectionMode in redirection settings: " + _Settings.Redirection.ReadRedirectionMode);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                            #endregion
                    }

                    #endregion
                }

                #endregion

                #endregion
            }
            finally
            {
                #region finally

                #region unlock

                if (locked)
                {
                    if (!_LockMgr.RemoveReadResource(md.CurrObj.DiskPath))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectHead unable to unlock " + md.CurrObj.DiskPath);
                    }
                }

                #endregion

                #endregion
            }
        }

        public HttpResponse Move(RequestMetadata md)
        {
            DateTime startTime = DateTime.Now;
            bool lockedOriginal = false;
            bool lockedTarget = false;
            string diskPathOriginal = "";
            string diskPathTarget = "";

            try
            {
                #region try

                #region Variables

                MoveRequest req = new MoveRequest();
                RestResponse proxyResponse = new RestResponse();
                Dictionary<string, string> restHeaders = new Dictionary<string, string>();
                Obj currObj = new Obj();
                bool userGatewayMode = false;
                List<Node> successfulReplicas = new List<Node>();
                string containerLogFile = "";
                string containerPropertiesFile = "";
                ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();
                string objectLogFile = "";
                string objectPropertiesFile = "";
                ObjectPropertiesFile currObjectPropertiesFile = new ObjectPropertiesFile();

                #endregion

                #region Check-Permissions

                if (md.CurrPerm == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove null ApiKeyPermission object supplied");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                }

                if (!Common.IsTrue(md.CurrPerm.AllowWriteObject))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove AllowWriteObject operation not authorized per permissions");
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
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.CreateObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteObject-Move", null));

                                    #endregion
                                }
                            }
                        }

                        if (currContainerPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ContainerPermission.GetPermission("WriteObject", md, currContainerPropertiesFile))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteObject-Move", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove AllowWriteObject operation not authorized per container permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Check-Object-Permissions-and-Logging

                    currObjectPropertiesFile = ObjectPropertiesFile.FromObject(md.CurrObj, out objectLogFile, out objectPropertiesFile);
                    if (currObjectPropertiesFile != null)
                    {
                        if (currObjectPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.CreateObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectMove", null));

                                    #endregion
                                }
                            }
                        }

                        if (currObjectPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ObjectPermission.GetPermission("WriteObject", md, currObjectPropertiesFile))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectMove", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove AllowWriteObject operation not authorized per object permissions");
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
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove null request after deserialization, returning 400");
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                        }
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unable to deserialize request body");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                    }

                    req.UserGuid = String.Copy(md.CurrUser.Guid);

                    #endregion

                    #region Validate-Request-Body

                    if (req.FromContainer == null)
                    {
                        req.FromContainer = new List<string>();
                    }

                    if (req.ToContainer == null)
                    {
                        req.ToContainer = new List<string>();
                    }

                    if (String.IsNullOrEmpty(req.MoveFrom))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove null value supplied for MoveFrom, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for MoveFrom.", null).ToJson(), true);
                    }

                    if (String.IsNullOrEmpty(req.MoveTo))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove null value supplied for MoveTo, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for MoveTo.", null).ToJson(), true);
                    }

                    if (FsHelper.ContainsUnsafeFsChars(req))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unsafe characters detected in request, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(2, 400, "Unsafe characters detected.", null).ToJson(), true);

                    }

                    req.UserGuid = md.CurrUser.Guid;

                    #endregion

                    #region Check-if-Original-Object-Exists

                    diskPathOriginal = _ObjMgr.BuildDiskPath(req, true, true);
                    if (String.IsNullOrEmpty(diskPathOriginal))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unable to build disk path for original object");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                    }

                    if (!Common.FileExists(diskPathOriginal))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove from object does not exist: " + diskPathOriginal);
                        return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                            new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                    }

                    #endregion

                    #region Lock-Original

                    lockedOriginal = _LockMgr.LockResource(md, diskPathOriginal);
                    if (!lockedOriginal)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove source object " + diskPathOriginal + " is unable to be locked");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(9, 423, "Resource in use.", null).ToJson(), true);
                    }

                    #endregion

                    #region Check-if-Target-Container-Exists

                    string diskPathTargetContainer = _ObjMgr.BuildDiskPath(req, false, false);
                    if (String.IsNullOrEmpty(diskPathTargetContainer))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unable to build disk path for target container");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to build disk path from target.", null).ToJson(), true);
                    }

                    if (!Common.DirectoryExists(diskPathTargetContainer))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove target container does not exist: " + diskPathOriginal);
                        return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                            new ErrorResponse(5, 404, "Container does not exist.", null).ToJson(), true);
                    }

                    #endregion

                    #region Check-if-Target-Object-Exists

                    diskPathTarget = _ObjMgr.BuildDiskPath(req, false, true);
                    if (String.IsNullOrEmpty(diskPathTarget))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unable to build disk path for target object");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                    }

                    if (Common.FileExists(diskPathTarget))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove target object already exists: " + diskPathOriginal);
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(2, 400, "Object already exists.", null).ToJson(), true);
                    }

                    #endregion

                    #region Lock-Target

                    lockedTarget = _LockMgr.LockResource(md, diskPathTarget);
                    if (!lockedTarget)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove target object " + diskPathTarget + " is unable to be locked");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(9, 423, "Resource in use.", null).ToJson(), true);
                    }

                    #endregion

                    #region Set-Gateway-Mode

                    userGatewayMode = md.CurrUser.GetGatewayMode(_Settings);

                    #endregion

                    #region Read-Original-Object

                    currObj = _ObjMgr.BuildFromDisk(diskPathOriginal);
                    if (currObj == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unable to retrieve obj for " + md.CurrObj.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to read object.", null).ToJson(), true);
                    }

                    #endregion

                    #region Process-Replication

                    if (!_Replication.ObjectMove(req, currObj))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove negative response from replication, returning 500");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to process replication.", null).ToJson(), true);
                    }

                    _Bunker.ObjectMove(req);

                    #endregion

                    #region Perform-Move

                    if (!Common.MoveFile(diskPathOriginal, diskPathTarget))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unable to move file from " + diskPathOriginal + " to " + diskPathTarget);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Object already exists.", null).ToJson(), true);
                    }

                    #endregion

                    #region Perform-Background-Rewrite

                    if (!userGatewayMode)
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "ObjectMove spawning background task to rewrite object with correct metadata");
                        Task.Run(() => RewriteObject(diskPathTarget));
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

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove object is destined for a different machine but WriteRedirectionMode is none");
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                        #endregion

                        case "proxy":
                            #region proxy

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectMove proxying request to " + md.CurrObj.PrimaryUrlWithoutQs);

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
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove null response from proxy REST request to " + md.CurrObj.PrimaryUrlWithoutQs);
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectMove server response to proxy REST request: " + proxyResponse.StatusCode);
                            return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                        #endregion

                        case "redirect":
                            #region redirect

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectMove redirecting request to " + md.CurrObj.PrimaryUrlWithoutQs + " using status " + _Settings.Redirection.WriteRedirectHttpStatus);
                            Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                            redirectHeader.Add("location", md.CurrObj.PrimaryUrlWithQs);
                            return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.WriteRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.WriteRedirectString, true);

                        #endregion

                        default:
                            #region unknown

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unknown WriteRedirectionMode in redirection settings: " + _Settings.Redirection.WriteRedirectionMode);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                            #endregion
                    }

                    #endregion
                }

                #endregion

                #endregion
            }
            finally
            {
                #region finally

                #region unlock

                if (lockedOriginal)
                {
                    if (!_LockMgr.UnlockResource(md, diskPathOriginal))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unable to unlock source path " + diskPathOriginal);
                    }
                }

                if (lockedTarget)
                {
                    if (!_LockMgr.UnlockResource(md, diskPathTarget))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unable to unlock target path " + diskPathTarget);
                    }
                }

                #endregion

                #endregion
            }
        }

        public HttpResponse Read(RequestMetadata md)
        {
            DateTime startTime = DateTime.Now;
            bool locked = false;

            try
            {
                #region try

                #region Variables

                RestResponse proxyResponse = new RestResponse();
                Dictionary<string, string> restHeaders = new Dictionary<string, string>();
                List<string> urls = new List<string>();
                string redirectUrl = "";
                string metadataVal = "";
                bool metadataOnly = false;
                string proxiedVal = "";
                bool proxied = false;
                string readFromVal = "";
                int readFrom = 0;
                string countVal = "";
                int count = 0;
                string imsVal = "";
                DateTime imsDt = new DateTime(1970, 1, 1);
                bool ims = false;
                string publicUrl = "";
                PublicObj currPubfile = new PublicObj();
                ObjInfo currObjInfo = new ObjInfo();
                Obj currObj = new Obj();
                byte[] clear;
                string maxResultsStr = "";
                int maxResults = 0;
                string ContentType = "application/octet-stream";
                string containerLogFile = "";
                string containerPropertiesFile = "";
                ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();
                string objectLogFile = "";
                string objectPropertiesFile = "";
                ObjectPropertiesFile currObjectPropertiesFile = new ObjectPropertiesFile();
                bool resize = false;
                int resizeWdith = 0;
                int resizeHeight = 0;

                #endregion

                #region Check-Permissions

                if (md.CurrPerm == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead null ApiKeyPermission object supplied");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                }

                if (!Common.IsTrue(md.CurrPerm.AllowReadObject))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead AllowReadObject operation not authorized per permissions");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                }

                #endregion

                #region Check-for-Key-in-URL

                if (String.IsNullOrEmpty(md.CurrObj.Key))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unable to find object key in URL");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to find object key URL.", null).ToJson(), true);
                }

                #endregion

                #region Get-Values-from-Querystring

                maxResultsStr = md.CurrHttpReq.RetrieveHeaderValue("max_results");
                if (!String.IsNullOrEmpty(maxResultsStr))
                {
                    if (!Int32.TryParse(maxResultsStr, out maxResults))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead invalid value for max_results in querystring: " + maxResultsStr);
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

                readFromVal = md.CurrHttpReq.RetrieveHeaderValue("read_from");
                if (!String.IsNullOrEmpty(readFromVal))
                {
                    if (!Int32.TryParse(readFromVal, out readFrom))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead invalid value for read_from in querystring: " + readFromVal);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for read_from.", null).ToJson(), true);
                    }

                    if (readFrom < 0)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead invalid value for read_from (must be zero or greater): " + readFrom);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for read_from.", null).ToJson(), true);
                    }
                }

                countVal = md.CurrHttpReq.RetrieveHeaderValue("count");
                if (!String.IsNullOrEmpty(countVal))
                {
                    if (!Int32.TryParse(countVal, out count))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead invalid value for count in querystring: " + countVal);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for count.", null).ToJson(), true);
                    }

                    if (count < 1)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead invalid value for count (must be greater than zero): " + count);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for count.", null).ToJson(), true);
                    }
                }

                publicUrl = md.CurrHttpReq.RetrieveHeaderValue("public_url");

                resize = Common.IsTrue(md.CurrHttpReq.RetrieveHeaderValue("resize"));

                if (resize)
                {
                    if (!Int32.TryParse(md.CurrHttpReq.RetrieveHeaderValue("width"), out resizeWdith))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead invalid value for width in querystring");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for width.", null).ToJson(), true);
                    }

                    if (!Int32.TryParse(md.CurrHttpReq.RetrieveHeaderValue("height"), out resizeHeight))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead invalid value for height in querystring");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for height.", null).ToJson(), true);
                    }
                }

                #endregion

                #region Get-Values-from-Headers

                imsVal = md.CurrHttpReq.RetrieveHeaderValue("if-modified-since");
                if (!String.IsNullOrEmpty(imsVal))
                {
                    if (!DateTime.TryParse(imsVal, out imsDt))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead invalid value for If-Modified-Since header: " + imsVal);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for If-Modified-Since header.  Use MM/dd/yyyy hh:mm:ss.", null).ToJson(), true);
                    }

                    ims = true;
                }

                #endregion

                #region Retrieve-Specific-Object

                /*
                 * Nodes that proxy requests will append ?proxied=true to the request URL
                 * to notify the next recipient node to not proxy it further (otherwise
                 * an infinite loop of REST requests will be generated)
                 * 
                 */

                if (proxied || (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId))
                {
                    #region Local-Owner

                    #region Add-Lock

                    locked = _LockMgr.AddReadResource(md.CurrObj.DiskPath);
                    if (!locked)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead " + md.CurrObj.DiskPath + " is unable to be locked");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(9, 423, "Resource in use.", null).ToJson(), true);
                    }

                    #endregion

                    #region Check-Container-Permissions-and-Logging

                    currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                    if (currContainerPropertiesFile != null)
                    {
                        if (currContainerPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.ReadObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "ReadObject", null));

                                    #endregion
                                }
                            }
                        }

                        if (currContainerPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ContainerPermission.GetPermission("ReadObject", md, currContainerPropertiesFile))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "ReadObject", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead AllowReadObject operation not authorized per container permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Check-Object-Permissions-and-Logging

                    currObjectPropertiesFile = ObjectPropertiesFile.FromObject(md.CurrObj, out objectLogFile, out objectPropertiesFile);
                    if (currObjectPropertiesFile != null)
                    {
                        if (currObjectPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.ReadObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectRead", null));

                                    #endregion
                                }
                            }
                        }

                        if (currObjectPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ObjectPermission.GetPermission("ReadObject", md, currObjectPropertiesFile))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectRead", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead AllowReadObject operation not authorized per object permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Retrieve-Object-Metadata

                    currObjInfo = ObjInfo.FromFile(md.CurrObj.DiskPath);
                    if (currObjInfo == null)
                    { 
                        return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                            new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                    }

                    #endregion

                    #region If-Modified-Since

                    if (ims)
                    {
                        if (currObjInfo.LastUpdate != null)
                        {
                            if (Convert.ToDateTime(currObjInfo.LastUpdate) < imsDt)
                            {
                                _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead not 304 modified for " + md.CurrObj.DiskPath);
                                return new HttpResponse(md.CurrHttpReq, true, 304, Common.AddToDictionary("Date", currObjInfo.LastUpdate.ToString(), null), null, null, true);
                            }
                        }
                    }

                    #endregion

                    #region Retrieve-Object

                    currObj = _ObjMgr.BuildFromDisk(md.CurrObj.DiskPath);
                    if (currObj == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unable to retrieve obj for " + md.CurrObj.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to read object.", null).ToJson(), true);
                    }

                    md.CurrObj = currObj;

                    #endregion

                    #region Process-Pubfile

                    if (Common.IsTrue(publicUrl))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead generating public URL handle for obj " + md.CurrObj.DiskPath);
                        currPubfile = new PublicObj();
                        currPubfile.Guid = Guid.NewGuid().ToString();
                        currPubfile.Url = PublicObj.BuildUrl(currPubfile.Guid, _Node);
                        currPubfile.DiskPath = md.CurrObj.DiskPath;
                        currPubfile.IsObject = 1;
                        currPubfile.IsContainer = 0;
                        currPubfile.Created = DateTime.Now.ToUniversalTime();
                        currPubfile.Expiration = currPubfile.Created.AddSeconds(_Settings.PublicObj.DefaultExpirationSec);
                        currPubfile.UserGuid = md.CurrUser.Guid;

                        if (!Common.WriteFile(_Settings.PublicObj.Directory + currPubfile.Guid, Encoding.UTF8.GetBytes(Common.SerializeJson(currPubfile))))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unable to create pubfile record for " + md.CurrObj.DiskPath);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to create public link.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead created pubfile record for " + currPubfile.DiskPath + " expiring " + currPubfile.Expiration.ToString("MM/dd/yyyy HH:mm:ss"));
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "text/plain", currPubfile.Url, true);
                    }

                    #endregion

                    #region Decrypt

                    if (Common.IsTrue(md.CurrObj.IsEncrypted))
                    {
                        if (Common.IsTrue(_Settings.Debug.DebugEncryption)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead before decryption: " + Common.BytesToBase64(md.CurrObj.Value));

                        if (String.IsNullOrEmpty(md.CurrObj.EncryptionKsn))
                        {
                            md.CurrObj.Value = _Encryption.LocalDecrypt(md.CurrObj.Value);
                        }
                        else
                        {
                            if (!_Encryption.ServerDecrypt(md.CurrObj.Value, md.CurrObj.EncryptionKsn, out clear))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unable to decrypt object using server-based decryption");
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to decrypt using crypto server.", null).ToJson(), true);
                            }

                            md.CurrObj.Value = clear;
                        }

                        if (Common.IsTrue(_Settings.Debug.DebugEncryption)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead after decryption: " + Common.BytesToBase64(md.CurrObj.Value));
                    }

                    #endregion

                    #region Decompress

                    if (Common.IsTrue(md.CurrObj.IsCompressed))
                    {
                        if (Common.IsTrue(_Settings.Debug.DebugCompression)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead before decompression: " + Common.BytesToBase64(md.CurrObj.Value));
                        md.CurrObj.Value = Common.GzipDecompress(md.CurrObj.Value);
                        if (Common.IsTrue(_Settings.Debug.DebugCompression)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead after decompression: " + Common.BytesToBase64(md.CurrObj.Value));
                    }

                    #endregion

                    #region Set-Content-Type

                    if (!String.IsNullOrEmpty(md.CurrObj.ContentType)) ContentType = md.CurrObj.ContentType;
                    else ContentType = MimeTypes.GetFromExtension(Common.GetFileExtension(md.CurrObj.DiskPath));

                    #endregion

                    #region Validate-Range-Read

                    if (count > 0)
                    {
                        if (readFrom + count > md.CurrObj.Value.Length)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead range exceeds object length (" + md.CurrObj.Value.Length + "): read_from " + readFrom + " count " + count);
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Range exceeds object length.", null).ToJson(), true);
                        }
                    }

                    #endregion

                    #region Respond

                    if (!metadataOnly)
                    {
                        #region Respond-with-Data

                        if (resize)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead attempting to resize object to width " + resizeWdith + " height " + resizeHeight);

                            if (Common.IsImage(md.CurrObj.Value))
                            {
                                Image original = Common.BytesToImage(md.CurrObj.Value);
                                if (original == null)
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unable to convert bytes to image");
                                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                        new ErrorResponse(1, 500, "Unable to convert BLOB to image.", null).ToJson(), true);
                                }

                                Image resized = Common.ResizeImage(original, resizeWdith, resizeHeight);
                                if (resized == null)
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unable to resize image");
                                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                        new ErrorResponse(1, 500, "Unable to resize image.", null).ToJson(), true);
                                }

                                md.CurrObj.Value = Common.ImageToBytes(resized);
                            }
                            else
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead byte data is not an image, returning original data");
                            }
                        }

                        if (count > 0)
                        {
                            byte[] ret = new byte[count];
                            Buffer.BlockCopy(md.CurrObj.Value, readFrom, ret, 0, count);
                            return new HttpResponse(md.CurrHttpReq, true, 200, null, ContentType, ret, true);
                        }
                        else
                        {
                            return new HttpResponse(md.CurrHttpReq, true, 200, null, ContentType, md.CurrObj.Value, true);
                        }

                        #endregion
                    }
                    else
                    {
                        #region Respond-with-Metadata

                        md.CurrObj.Value = null;
                        md.CurrObj.Created = currObjInfo.Created;
                        md.CurrObj.LastUpdate = currObjInfo.LastUpdate;
                        md.CurrObj.LastAccess = currObjInfo.LastAccess;
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(md.CurrObj), true);

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

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead object is stored on a different machine but ReadRedirectionMode is none");
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Request proxying disabled by configuration  Please direct this request to the appropriate node.", null).ToJson(), true);

                        #endregion

                        case "proxy":
                            #region proxy

                            if (_MaintenanceMgr.IsEnabled())
                            {
                                urls = _ObjMgr.MaintenanceUrls(true, md.CurrHttpReq, md.CurrObj);
                            }
                            else
                            {
                                urls = _ObjMgr.ReplicaUrls(true, md.CurrHttpReq, md.CurrObj);
                            }

                            if (urls == null || urls.Count < 1)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unable to build replica URL list (null response)");
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to build proxy URL.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead proxying request to " + urls.Count + " URLs for user GUID " + md.CurrUser.Guid);

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
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead null response from proxy REST request to " + urls.Count + " URLs");
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead server response to proxy REST request: " + proxyResponse.StatusCode);
                            return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                        #endregion

                        case "redirect":
                            #region redirect

                            redirectUrl = _ObjMgr.RedirectUrl(true, md.CurrHttpReq, md.CurrObj);
                            if (String.IsNullOrEmpty(redirectUrl))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unable to generate redirect_url, returning 500");
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to build redirect URL.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectRead redirecting request using status " + _Settings.Redirection.ReadRedirectHttpStatus + " for user GUID " + md.CurrUser.Guid);
                            Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                            redirectHeader.Add("location", redirectUrl);
                            return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.ReadRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.ReadRedirectString, true);

                        #endregion

                        default:
                            #region unknown

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unknown ReadRedirectionMode in redirection settings: " + _Settings.Redirection.ReadRedirectionMode);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                            #endregion
                    }

                    #endregion
                }

                #endregion

                #endregion
            }
            finally
            {
                #region finally

                #region unlock

                if (locked)
                {
                    if (!_LockMgr.RemoveReadResource(md.CurrObj.DiskPath))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRead unable to unlock " + md.CurrObj.DiskPath);
                    }
                }

                #endregion

                #endregion
            }
        }

        public HttpResponse Rename(RequestMetadata md)
        {
            DateTime startTime = DateTime.Now;
            bool lockedOriginal = false;
            bool lockedTarget = false;
            string diskPathOriginal = "";
            string diskPathTarget = "";

            try
            {
                #region try

                #region Variables

                RenameRequest req = new RenameRequest();
                RestResponse proxyResponse = new RestResponse();
                Dictionary<string, string> restHeaders = new Dictionary<string, string>();
                bool userGatewayMode = false;
                Obj currObj = new Obj();
                List<Node> successfulReplicas = new List<Node>();
                string containerLogFile = "";
                string containerPropertiesFile = "";
                ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();
                string objectLogFile = "";
                string objectPropertiesFile = "";
                ObjectPropertiesFile currObjectPropertiesFile = new ObjectPropertiesFile();

                #endregion

                #region Check-Permissions

                if (md.CurrPerm == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename null ApiKeyPermission object supplied");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                }

                if (!Common.IsTrue(md.CurrPerm.AllowWriteObject))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename AllowWriteObject operation not authorized per permissions");
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
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.CreateObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteObject-Rename", null));

                                    #endregion
                                }
                            }
                        }

                        if (currContainerPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ContainerPermission.GetPermission("WriteObject", md, currContainerPropertiesFile))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteObject-Rename", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename AllowWriteObject operation not authorized per container permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Check-Object-Permissions-and-Logging

                    currObjectPropertiesFile = ObjectPropertiesFile.FromObject(md.CurrObj, out objectLogFile, out objectPropertiesFile);
                    if (currObjectPropertiesFile != null)
                    {
                        if (currObjectPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.CreateObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectRename", null));

                                    #endregion
                                }
                            }
                        }

                        if (currObjectPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ObjectPermission.GetPermission("WriteObject", md, currObjectPropertiesFile))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectRename", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename AllowWriteObject operation not authorized per object permissions");
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
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename null request after deserialization, returning 400");
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                        }
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unable to deserialize request body");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                    }

                    req.UserGuid = String.Copy(md.CurrUser.Guid);

                    #endregion

                    #region Validate-Request-Body

                    if (req.ContainerPath == null)
                    {
                        req.ContainerPath = new List<string>();
                    }

                    if (String.IsNullOrEmpty(req.RenameFrom))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename null value supplied for RenameFrom, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for RenameFrom.", null).ToJson(), true);
                    }

                    if (String.IsNullOrEmpty(req.RenameTo))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename null value supplied for RenameTo, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for RenameTo.", null).ToJson(), true);
                    }

                    if (FsHelper.ContainsUnsafeFsChars(req))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unsafe characters detected in request, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unsafe characters detected.", null).ToJson(), true);
                    }

                    req.UserGuid = md.CurrUser.Guid;

                    #endregion

                    #region Check-if-Original-Exists

                    diskPathOriginal = _ObjMgr.BuildDiskPath(req, true);
                    if (String.IsNullOrEmpty(diskPathOriginal))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unable to build disk path for original object");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                    }

                    if (!Common.FileExists(diskPathOriginal))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename from object does not exist: " + diskPathOriginal);
                        return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                            new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                    }

                    #endregion

                    #region Lock-Original

                    lockedOriginal = _LockMgr.LockResource(md, diskPathOriginal);
                    if (!lockedOriginal)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename source object " + diskPathOriginal + " is unable to be locked");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(9, 423, "Resource in use.", null).ToJson(), true);
                    }

                    #endregion

                    #region Check-if-Target-Exists

                    diskPathTarget = _ObjMgr.BuildDiskPath(req, false);
                    if (String.IsNullOrEmpty(diskPathTarget))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unable to build disk path for target object");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                    }

                    if (Common.FileExists(diskPathTarget))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename target object already exists: " + diskPathOriginal);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Object already exists.", null).ToJson(), true);
                    }

                    #endregion

                    #region Lock-Target

                    lockedTarget = _LockMgr.LockResource(md, diskPathTarget);
                    if (!lockedTarget)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename target object " + diskPathTarget + " is unable to be locked");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(9, 423, "Resource in use.", null).ToJson(), true);
                    }

                    #endregion

                    #region Set-Gateway-Mode

                    userGatewayMode = md.CurrUser.GetGatewayMode(_Settings);

                    #endregion

                    #region Read-Original-Object

                    currObj = _ObjMgr.BuildFromDisk(diskPathOriginal);
                    if (currObj == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unable to retrieve obj for " + md.CurrObj.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to read object.", null).ToJson(), true);
                    }

                    #endregion

                    #region Process-Replication

                    if (!_Replication.ObjectRename(req, currObj))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename negative response from replication, returning 500");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to process replication.", null).ToJson(), true);
                    }

                    _Bunker.ObjectRename(req);

                    #endregion

                    #region Perform-Rename

                    if (!Common.RenameFile(diskPathOriginal, diskPathTarget))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unable to rename file from " + diskPathOriginal + " to " + diskPathTarget);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Object already exists.", null).ToJson(), true);
                    }

                    #endregion

                    #region Perform-Background-Rewrite

                    if (!userGatewayMode)
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "ObjectRename spawning background task to rewrite object with correct metadata");
                        Task.Run(() => RewriteObject(diskPathTarget));
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

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename object is destined for a different machine but WriteRedirectionMode is none");
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                        #endregion

                        case "proxy":
                            #region proxy

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectRename proxying request to " + md.CurrObj.PrimaryUrlWithoutQs);

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
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename null response from proxy REST request to " + md.CurrObj.PrimaryUrlWithoutQs);
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectRename server response to proxy REST request: " + proxyResponse.StatusCode);
                            return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                        #endregion

                        case "redirect":
                            #region redirect

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectRename redirecting request to " + md.CurrObj.PrimaryUrlWithoutQs + " using status " + _Settings.Redirection.WriteRedirectHttpStatus);
                            Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                            redirectHeader.Add("location", md.CurrObj.PrimaryUrlWithQs);
                            return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.WriteRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.WriteRedirectString, true);

                        #endregion

                        default:
                            #region unknown

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unknown WriteRedirectionMode in redirection settings: " + _Settings.Redirection.WriteRedirectionMode);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Server has incorrect proxy configuration.", null).ToJson(), true);

                            #endregion
                    }

                    #endregion
                }

                #endregion

                #endregion
            }
            finally
            {
                #region finally

                #region unlock

                if (lockedOriginal)
                {
                    if (!_LockMgr.UnlockResource(md, diskPathOriginal))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unable to unlock source path " + diskPathOriginal);
                    }
                }

                if (lockedTarget)
                {
                    if (!_LockMgr.UnlockResource(md, diskPathTarget))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unable to unlock target path " + diskPathTarget);
                    }
                }

                #endregion

                #endregion
            }
        }

        public HttpResponse Search(RequestMetadata md)
        {
            #region Variables

            string maxResultsStr = "";
            int maxResults = 0;
            string metadataVal = "";
            bool metadataOnly = false;
            Find req = new Find();
            string diskPath = "";
            RestResponse proxyResponse = new RestResponse();
            Dictionary<string, string> restHeaders = new Dictionary<string, string>();
            DirInfo ret = new DirInfo();
            List<DirInfo> retList = new List<DirInfo>();
            List<string> subdirList = new List<string>();
            List<string> urls = new List<string>();
            string redirectUrl = "";
            string proxiedVal = "";
            bool proxied = false;

            #endregion

            #region Check-Permissions

            if (md.CurrPerm == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch null ApiKeyPermission object supplied");
                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
            }

            if (!Common.IsTrue(md.CurrPerm.AllowSearch))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch allow_search operation not authorized per permissions");
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
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch invalid value for max_results in querystring: " + maxResultsStr);
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid value for max_results.", null).ToJson(), true);
                }
            }
            else
            {
                maxResults = 1;
            }

            metadataVal = md.CurrHttpReq.RetrieveHeaderValue("metadata_only");
            if (!String.IsNullOrEmpty(metadataVal))
            {
                metadataOnly = Common.IsTrue(metadataVal);
            }

            proxiedVal = md.CurrHttpReq.RetrieveHeaderValue("proxied");
            if (!String.IsNullOrEmpty(proxiedVal))
            {
                proxied = Common.IsTrue(proxiedVal);
            }

            #endregion

            #region Process-Owner

            /*
                * Nodes that proxy requests will append ?proxied=true to the request URL
                * to notify the next recipient node to not proxy it further (otherwise
                * an infinite loop of REST requests will be generated)
                * 
                */

            if (proxied || (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId))
            {
                #region Local-Owner

                #region Deserialize

                try
                {
                    req = Common.DeserializeJson<Find>(md.CurrHttpReq.Data);
                    if (req == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch null request after deserialization, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                    }
                }
                catch (Exception)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch unable to deserialize request body");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                }

                req.UserGuid = md.CurrObj.UserGuid;

                #endregion

                #region Process-and-Return

                diskPath = _ObjMgr.BuildDiskPath(req);
                if (String.IsNullOrEmpty(diskPath))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch unable to build disk path from request body");
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                }

                if (req.Recursive)
                {
                    #region Recursive

                    #region Build-and-Process-Subdirectory-List

                    if (!String.IsNullOrEmpty(req.Key)) subdirList = Common.GetSubdirectoryList(diskPath.Replace(req.Key, ""), true);
                    else subdirList = Common.GetSubdirectoryList(diskPath, true);

                    if (subdirList == null) subdirList = new List<string>();

                    if (!String.IsNullOrEmpty(req.Key)) subdirList.Add(diskPath.Replace(req.Key, ""));
                    else subdirList.Add(diskPath);

                    int resultsCount = maxResults;
                    foreach (string currSubdir in subdirList)
                    { 
                        DirInfo di = new DirInfo(_Settings, _UserMgr, _Logging);
                        ret = di.FromDirectory(currSubdir, md.CurrObj.UserGuid, resultsCount, req.Filters, metadataOnly);
                        if (ret == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch null directory info for " + currSubdir);
                            return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                                new ErrorResponse(5, 404, "Container does not exist.", null).ToJson(), true);
                        }

                        // do not add to list if no objects are present
                        if (ret.NumObjects <= 0) continue;

                        ret.UserGuid = md.CurrUser.Guid;
                        ret.ContainerPath = di.GetContainerList(currSubdir, md.CurrUser.Guid);

                        maxResults -= ret.NumObjects;
                        retList.Add(ret);

                        if (maxResults <= 0)
                        {
                            break;
                        }
                    }

                    #endregion

                    return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(retList), true);

                    #endregion
                }
                else
                {
                    #region Non-Recursive

                    DirInfo di = new DirInfo(_Settings, _UserMgr, _Logging);
                    ret = di.FromDirectory(
                        _ObjMgr.BuildDiskPath(req), 
                        md.CurrObj.UserGuid, maxResults, req.Filters, metadataOnly);

                    if (ret == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch null directory info for container");
                        return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                            new ErrorResponse(5, 404, "Container does not exist.", null).ToJson(), true);
                    }

                    ret.UserGuid = md.CurrUser.Guid;
                    ret.ContainerPath = req.ContainerPath;

                    return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(ret), true);

                    #endregion
                }

                #endregion

                #endregion
            }
            else
            {
                #region Remote-Owner

                switch (_Settings.Redirection.SearchRedirectionMode)
                {
                    case "none":
                        #region none

                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch object is destined for a different machine but SearchRedirectionMode is none");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                    #endregion

                    case "proxy":
                        #region proxy

                        urls = _ObjMgr.ReplicaUrls(true, md.CurrHttpReq, md.CurrObj);
                        if (urls == null || urls.Count < 1)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch unable to build replica URL list (null response)");
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to build proxy URL.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ObjectSearch proxying request to " + urls.Count + " URLs for user GUID " + md.CurrUser.Guid);

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
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch null response from proxy REST request to " + urls.Count + " URLs");
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ObjectSearch server response to proxy REST request: " + proxyResponse.StatusCode);
                        return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                    #endregion

                    case "redirect":
                        #region redirect

                        redirectUrl = _ObjMgr.RedirectUrl(true, md.CurrHttpReq, md.CurrObj);
                        if (String.IsNullOrEmpty(redirectUrl))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch unable to generate redirect URL, returning 500");
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to build redirect URL.", null).ToJson(), true);
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "ObjectSearch redirecting request using status " + _Settings.Redirection.ReadRedirectHttpStatus + " for user GUID " + md.CurrUser.Guid);
                        Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                        redirectHeader.Add("location", redirectUrl);
                        return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.ReadRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.ReadRedirectString, true);

                    #endregion

                    default:
                        #region unknown

                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectSearch unknown SearchRedirectionMode in redirection settings: " + _Settings.Redirection.SearchRedirectionMode);
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
            DateTime startTime = DateTime.Now;
            bool locked = false;

            try
            {
                #region try

                #region Variables

                ObjInfo currObjInfo = new ObjInfo();
                Obj currObj = new Obj();
                string rangeWriteVal = "";
                bool rangeWrite = false;
                string writeToVal = "";
                int writeTo = 0;
                string overwriteVal = "";
                bool overwrite = false;
                string homeDirectory = "";
                byte[] clear;
                byte[] cipher;
                string ksn = "";
                string baseDir = "";
                int guidAttempts = 0;
                RestResponse proxyResponse = new RestResponse();
                Dictionary<string, string> restHeaders = new Dictionary<string, string>();
                int expirationSeconds = 0;
                string expirationFilename = "";
                Obj expObj;
                bool localSuccess = false;
                List<Node> successfulReplicas = new List<Node>();
                string containerLogFile = "";
                string containerPropertiesFile = "";
                ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();
                string objectLogFile = "";
                string objectPropertiesFile = "";
                ObjectPropertiesFile currObjectPropertiesFile = new ObjectPropertiesFile();

                #endregion

                #region Check-Permissions

                if (md.CurrPerm == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite null ApiKeyPermission object supplied");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                }

                if (!Common.IsTrue(md.CurrPerm.AllowWriteObject))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite AllowWriteObject operation not authorized per permissions");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                }

                #endregion

                #region Check-Size

                if (md.CurrObj.Value == null || md.CurrObj.Value.Length < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite null object value detected");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "No request body.", null).ToJson(), true);
                }

                if (md.CurrObj.Value.Length > _Settings.Storage.MaxObjectSize)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite object size of " + md.CurrObj.Value.Length + " exceeds configured max_object_size of " + _Settings.Storage.MaxObjectSize);
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Request body is too large.", null).ToJson(), true);
                }

                #endregion

                #region Retrieve-User-Home-Directory

                homeDirectory = _UserMgr.GetHomeDirectory(md.CurrUser.Guid, _Settings);
                if (String.IsNullOrEmpty(homeDirectory))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to retrieve home directory for user GUID " + md.CurrUser.Guid);
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to find home directory for user.", null).ToJson(), true);
                }

                #endregion

                #region Process-Owner

                if (md.CurrObj.PrimaryNode.NodeId == _Node.NodeId)
                {
                    #region Local-Owner

                    #region Generate-New-Key-if-Needed

                    if (String.IsNullOrEmpty(md.CurrObj.Key))
                    {
                        while (true)
                        {
                            md.CurrObj.Key = Guid.NewGuid().ToString();
                            if (!Common.FileExists(md.CurrObj.DiskPath + md.CurrObj.Key))
                            {
                                #region Amend-Path-Object-With-New-URL

                                md.CurrObj.PrimaryUrlWithQs = _ObjMgr.PrimaryUrl(true, md.CurrHttpReq, md.CurrObj);
                                if (String.IsNullOrEmpty(md.CurrObj.PrimaryUrlWithQs))
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to build primary URL for request (with querystring)");
                                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                        new ErrorResponse(4, 500, "Unable to build primary URL.", null).ToJson(), true);
                                }

                                md.CurrObj.PrimaryUrlWithoutQs = _ObjMgr.PrimaryUrl(false, md.CurrHttpReq, md.CurrObj);
                                if (String.IsNullOrEmpty(md.CurrObj.PrimaryUrlWithoutQs))
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to build primary URL for request (without querystring)");
                                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                        new ErrorResponse(4, 500, "Unable to build primary URL.", null).ToJson(), true);
                                }

                                md.CurrObj.DiskPath = _ObjMgr.DiskPath(md.CurrObj, md.CurrUser);
                                if (String.IsNullOrEmpty(md.CurrObj.DiskPath))
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to build disk path for request");
                                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                        new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                                }

                                _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite overwriting path values (object had no key originally)");
                                _Logging.Log(LoggingModule.Severity.Debug, "  Key         : " + md.CurrObj.Key);
                                _Logging.Log(LoggingModule.Severity.Debug, "  URL (no qs) : " + md.CurrObj.PrimaryUrlWithoutQs);
                                _Logging.Log(LoggingModule.Severity.Debug, "  URL (qs)    : " + md.CurrObj.PrimaryUrlWithQs);
                                _Logging.Log(LoggingModule.Severity.Debug, "  Disk Path   : " + md.CurrObj.DiskPath);

                                #endregion

                                break;
                            }

                            guidAttempts++;
                            if (guidAttempts >= 8)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite cannot get a new GUID");
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to find unused GUID.", null).ToJson(), true);
                            }
                        }
                    }

                    #endregion

                    #region Add-Lock

                    locked = _LockMgr.LockUrl(md);
                    if (!locked)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite " + md.CurrObj.DiskPath + " is unable to be locked");
                        return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                            new ErrorResponse(9, 423, "Resource in use.", null).ToJson(), true);
                    }

                    #endregion

                    #region Check-Container-Permissions-and-Logging

                    currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                    if (currContainerPropertiesFile != null)
                    {
                        if (currContainerPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.CreateObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteObject", null));

                                    #endregion
                                }
                            }
                        }

                        if (currContainerPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ContainerPermission.GetPermission("WriteObject", md, currContainerPropertiesFile))
                            {
                                if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "WriteObject", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite AllowWriteObject operation not authorized per container permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Check-Object-Permissions-and-Logging

                    currObjectPropertiesFile = ObjectPropertiesFile.FromObject(md.CurrObj, out objectLogFile, out objectPropertiesFile);
                    if (currObjectPropertiesFile != null)
                    {
                        if (currObjectPropertiesFile.Logging != null)
                        {
                            if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.CreateObject))
                                {
                                    #region Process-Logging

                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectWrite", null));

                                    #endregion
                                }
                            }
                        }

                        if (currObjectPropertiesFile.Permissions != null)
                        {
                            #region Evaluate-Permissions

                            if (!ObjectPermission.GetPermission("WriteObject", md, currObjectPropertiesFile))
                            {
                                if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                                {
                                    _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "ObjectWrite", "denied"));
                                }

                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite AllowWriteObject operation not authorized per object permissions");
                                return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                                    new ErrorResponse(3, 401, "Permission denied.", null).ToJson(), true);
                            }

                            #endregion
                        }
                    }

                    #endregion

                    #region Validate-Request

                    if (FsHelper.ContainsUnsafeFsChars(md.CurrObj))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unsafe characters detected in request, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unsafe characters detected.", null).ToJson(), true);
                    }

                    #endregion

                    #region Get-Values-from-Querystring

                    rangeWriteVal = md.CurrHttpReq.RetrieveHeaderValue("range_write");
                    if (!String.IsNullOrEmpty(rangeWriteVal))
                    {
                        rangeWrite = Common.IsTrue(rangeWriteVal);
                    }

                    writeToVal = md.CurrHttpReq.RetrieveHeaderValue("write_to");
                    if (!String.IsNullOrEmpty(writeToVal))
                    {
                        if (!Int32.TryParse(writeToVal, out writeTo))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite invalid value for write_to in querystring: " + writeToVal);
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Invalid value for write_to.", null).ToJson(), true);
                        }

                        if (writeTo < 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite invalid value for write_to (must be zero or greater): " + writeTo);
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Invalid value for write_to.", null).ToJson(), true);
                        }
                    }

                    overwriteVal = md.CurrHttpReq.RetrieveHeaderValue("overwrite");
                    if (!String.IsNullOrEmpty(overwriteVal))
                    {
                        Boolean.TryParse(overwriteVal, out overwrite);
                    }

                    #endregion

                    #region Create-User-Folder-if-Needed

                    if (!Common.DirectoryExists(homeDirectory))
                    {
                        Common.CreateDirectory(homeDirectory);
                    }

                    #endregion

                    #region Create-Containers-if-Needed

                    baseDir = String.Copy(homeDirectory);

                    foreach (string currDirectory in md.CurrObj.ContainerPath)
                    {
                        baseDir += Common.GetPathSeparator(_Settings.Environment) + currDirectory;

                        if (!Common.DirectoryExists(baseDir))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite directory " + baseDir + " does not exist, creating");
                            if (!Common.CreateDirectory(baseDir))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to create base directory " + baseDir);
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to write container.", null).ToJson(), true);
                            }
                        }
                    }

                    #endregion

                    #region Set-Content-Type-if-Needed

                    if (String.IsNullOrEmpty(md.CurrObj.ContentType))
                    {
                        md.CurrObj.ContentType = MimeTypes.GetFromExtension(Common.GetFileExtension(md.CurrObj.DiskPath));
                    }

                    #endregion

                    #region Handle-Range-Write-and-Existing-File

                    if (rangeWrite)
                    {
                        #region Update-Existing-File

                        #region Ensure-Body-Present

                        if (md.CurrHttpReq.Data.Length < 1)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite null request body supplied for range write");
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Request body not found.", null).ToJson(), true);
                        }

                        #endregion

                        #region Retrieve-Object-Metadata

                        currObjInfo = ObjInfo.FromFile(md.CurrObj.DiskPath);
                        if (currObjInfo == null)
                        { 
                            return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                                new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                        }

                        #endregion

                        #region Read-Original

                        currObj = _ObjMgr.BuildFromDisk(md.CurrObj.DiskPath);
                        if (currObj == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to retrieve obj for " + md.CurrObj.DiskPath);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to read object.", null).ToJson(), true);
                        }

                        md.CurrObj = currObj;

                        #endregion

                        #region Decrypt

                        if (Common.IsTrue(md.CurrObj.IsEncrypted))
                        {
                            if (Common.IsTrue(_Settings.Debug.DebugEncryption)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite before decryption: " + Common.BytesToBase64(md.CurrObj.Value));

                            if (String.IsNullOrEmpty(md.CurrObj.EncryptionKsn))
                            {
                                md.CurrObj.Value = _Encryption.LocalDecrypt(md.CurrObj.Value);
                            }
                            else
                            {
                                if (!_Encryption.ServerDecrypt(md.CurrObj.Value, md.CurrObj.EncryptionKsn, out clear))
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to decrypt object using server-based decryption");
                                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                        new ErrorResponse(4, 500, "Unable to decrypt object using crypto server.", null).ToJson(), true);
                                }

                                md.CurrObj.Value = clear;
                            }

                            if (Common.IsTrue(_Settings.Debug.DebugEncryption)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite after decryption: " + Common.BytesToBase64(md.CurrObj.Value));
                        }

                        #endregion

                        #region Decompress

                        if (Common.IsTrue(md.CurrObj.IsCompressed))
                        {
                            if (Common.IsTrue(_Settings.Debug.DebugCompression)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite before decompression: " + Common.BytesToBase64(md.CurrObj.Value));
                            md.CurrObj.Value = Common.GzipDecompress(md.CurrObj.Value);
                            if (Common.IsTrue(_Settings.Debug.DebugCompression)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite after decompression: " + Common.BytesToBase64(md.CurrObj.Value));
                        }

                        #endregion

                        #region Set-Content-Type

                        if (String.IsNullOrEmpty(md.CurrObj.ContentType))
                        {
                            md.CurrObj.ContentType = MimeTypes.GetFromExtension(Common.GetFileExtension(md.CurrObj.DiskPath));
                        }

                        #endregion

                        #region Check-if-in-Range

                        if (writeTo < md.CurrObj.Value.Length)
                        {
                            if (writeTo + md.CurrHttpReq.Data.Length > md.CurrObj.Value.Length)
                            {
                                #region Replace-then-Append

                                // copy original from obj.value to a bigger byte array
                                byte[] newObjBytes = new byte[(writeTo + md.CurrHttpReq.Data.Length)];
                                Buffer.BlockCopy(md.CurrObj.Value, 0, newObjBytes, 0, md.CurrObj.Value.Length);

                                // add new data from http rqeuest
                                Buffer.BlockCopy(md.CurrHttpReq.Data, 0, newObjBytes, writeTo, md.CurrHttpReq.Data.Length);

                                // copy from bigger byte array back to obj.value
                                md.CurrObj.Value = new byte[newObjBytes.Length];
                                Buffer.BlockCopy(newObjBytes, 0, md.CurrObj.Value, 0, newObjBytes.Length);

                                #endregion
                            }
                            else
                            {
                                #region Replace

                                // in-place replacement
                                Buffer.BlockCopy(md.CurrHttpReq.Data, 0, md.CurrObj.Value, writeTo, md.CurrHttpReq.Data.Length);

                                #endregion
                            }
                        }
                        else if (writeTo >= md.CurrObj.Value.Length)
                        {
                            #region Append

                            // copy original from obj.value to a bigger byte array
                            byte[] newObjBytes = new byte[(writeTo + md.CurrHttpReq.Data.Length)];
                            Buffer.BlockCopy(md.CurrObj.Value, 0, newObjBytes, 0, md.CurrObj.Value.Length);

                            // add new data from http rqeuest
                            Buffer.BlockCopy(md.CurrHttpReq.Data, 0, newObjBytes, writeTo, md.CurrHttpReq.Data.Length);

                            // copy from bigger byte array back to obj.value
                            md.CurrObj.Value = new byte[newObjBytes.Length];
                            Buffer.BlockCopy(newObjBytes, 0, md.CurrObj.Value, 0, newObjBytes.Length);

                            #endregion
                        }

                        #endregion

                        #endregion
                    }
                    else
                    {
                        #region Check-for-Existing-File

                        if (!overwrite)
                        {
                            if (Common.FileExists(md.CurrObj.DiskPath))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite file already exists at " + md.CurrObj.DiskPath);
                                return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                    new ErrorResponse(2, 400, "Object already exists.", null).ToJson(), true);
                            }
                        }

                        #endregion
                    }

                    #endregion

                    #region Write-Expiration-Object

                    expirationSeconds = md.CurrUser.GetExpirationSeconds(_Settings, md.CurrApiKey);
                    if (expirationSeconds > 0)
                    {
                        expObj = Common.CopyObject<Obj>(md.CurrObj);
                        expObj.Value = null;
                        expObj.Expiration = DateTime.Now.AddSeconds(expirationSeconds);
                        md.CurrObj.Expiration = expObj.Expiration;

                        expirationFilename =
                            Convert.ToDateTime(expObj.Expiration).ToString("MMddyyyy-hhmmss") +
                            "-" + Common.RandomString(8) + "-" + expObj.Key;

                        if (!Common.WriteFile(_Settings.Expiration.Directory + expirationFilename, Common.SerializeJson(expObj), false))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to create expiration object " + expirationFilename);
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to create expiration object.", null).ToJson(), true);
                        }
                    }

                    #endregion

                    #region Compress

                    if (!Common.IsTrue(md.CurrObj.GatewayMode))
                    {
                        if (Common.IsTrue(md.CurrObj.IsCompressed))
                        {
                            if (Common.IsTrue(_Settings.Debug.DebugCompression)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite before compression: " + Common.BytesToBase64(md.CurrObj.Value));
                            md.CurrObj.Value = Common.GzipCompress(md.CurrObj.Value);
                            if (Common.IsTrue(_Settings.Debug.DebugCompression)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite after compression: " + Common.BytesToBase64(md.CurrObj.Value));
                        }
                    }
                    else
                    {
                        md.CurrObj.IsCompressed = 0;
                    }

                    #endregion

                    #region Encrypt

                    if (!Common.IsTrue(md.CurrObj.GatewayMode))
                    {
                        if (Common.IsTrue(md.CurrObj.IsEncrypted))
                        {
                            if (Common.IsTrue(_Settings.Debug.DebugEncryption)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite before encryption: " + Common.BytesToBase64(md.CurrObj.Value));

                            switch (_Settings.Encryption.Mode)
                            {
                                case "local":
                                    #region local

                                    md.CurrObj.Value = _Encryption.LocalEncrypt(md.CurrObj.Value);
                                    break;

                                #endregion

                                case "server":
                                    #region server

                                    if (!_Encryption.ServerEncrypt(md.CurrObj.Value, out cipher, out ksn))
                                    {
                                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to encrypt object using server-based encryption");
                                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                            new ErrorResponse(4, 500, "Unable to encrypt object using crypto server.", null).ToJson(), true);
                                    }

                                    md.CurrObj.Value = cipher;
                                    md.CurrObj.EncryptionKsn = ksn;
                                    break;

                                #endregion

                                default:
                                    #region default

                                    _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unknown encryption mode: " + _Settings.Encryption.Mode);
                                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                        new ErrorResponse(4, 500, "Server has incorrect encryption configuration.", null).ToJson(), true);

                                    #endregion
                            }

                            if (Common.IsTrue(_Settings.Debug.DebugEncryption)) _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite after encryption: " + Common.BytesToBase64(md.CurrObj.Value));
                        }
                    }
                    else
                    {
                        md.CurrObj.IsEncrypted = 0;
                    }

                    #endregion

                    #region Process-Replication

                    if (!_Replication.ObjectWrite(md.CurrObj, out successfulReplicas))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite negative response from replication, returning 500");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to process replication.", null).ToJson(), true);
                    }

                    _Bunker.ObjectWrite(md.CurrObj);

                    #endregion

                    #region Write-Locally

                    if (!Common.IsTrue(md.CurrObj.GatewayMode))
                    {
                        localSuccess = Common.WriteFile(md.CurrObj.DiskPath, Common.SerializeJson(md.CurrObj), false);
                        if (!localSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to write object to " + md.CurrUser.Guid + " " + md.CurrObj.Key);
                            Task.Run(() =>
                            {
                                _Replication.ObjectDelete(md.CurrObj, successfulReplicas);
                            });

                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to write object.", null).ToJson(), true);
                        }
                    }
                    else
                    {
                        localSuccess = Common.WriteFile(md.CurrObj.DiskPath, md.CurrObj.Value);
                        if (!localSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to write raw bytes to " + md.CurrUser.Guid + " " + md.CurrObj.Key);
                            Task.Run(() =>
                            {
                                _Replication.ObjectDelete(md.CurrObj, successfulReplicas);
                            });

                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to write object.", null).ToJson(), true);
                        }
                    }

                    _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite successfully wrote " + md.CurrObj.Key + " for " + md.CurrUser.Guid);

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

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite object is destined for a different machine but WriteRedirectionMode is none");
                            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                                new ErrorResponse(2, 400, "Request proxying disabled by configuration.  Please direct this request to the appropriate node.", null).ToJson(), true);

                        #endregion

                        case "proxy":
                            #region proxy

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite proxying request to " + md.CurrObj.PrimaryUrlWithoutQs + " for object key " + md.CurrObj.Key);

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
                                _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite null response from proxy REST request to " + md.CurrObj.PrimaryUrlWithoutQs);
                                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                    new ErrorResponse(4, 500, "Unable to communicate with the appropriate node for this request.", null).ToJson(), true);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite server response to proxy REST request: " + proxyResponse.StatusCode);
                            return new HttpResponse(md.CurrHttpReq, true, proxyResponse.StatusCode, proxyResponse.Headers, null, proxyResponse.Data, true);

                        #endregion

                        case "redirect":
                            #region redirect

                            _Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite redirecting request to " + md.CurrObj.PrimaryUrlWithoutQs +
                                " using status " + _Settings.Redirection.WriteRedirectHttpStatus + " for object key " + md.CurrObj.Key);
                            Dictionary<string, string> redirectHeader = new Dictionary<string, string>();
                            redirectHeader.Add("location", md.CurrObj.PrimaryUrlWithQs);
                            return new HttpResponse(md.CurrHttpReq, true, _Settings.Redirection.WriteRedirectHttpStatus, redirectHeader, null, _Settings.Redirection.WriteRedirectString, true);

                        #endregion

                        default:
                            #region unknown

                            _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unknown WriteRedirectionMode in redirection settings: " + _Settings.Redirection.WriteRedirectionMode);
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
                    _Logging.Log(LoggingModule.Severity.Alert, "ObjectWrite disk full detected during write operation for " + md.CurrUser.Guid + " " + md.CurrObj.Key);
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(1, 500, "Disk is full.", null).ToJson(), true);
                }

                #endregion

                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json", null, true);
            }
            finally
            {
                #region finally

                #region unlock

                if (locked)
                {
                    if (!_LockMgr.UnlockUrl(md))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unable to unlock " + md.CurrObj.DiskPath);
                    }
                }

                #endregion

                #endregion
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

            currObj = _ObjMgr.BuildFromDisk(filename);
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
