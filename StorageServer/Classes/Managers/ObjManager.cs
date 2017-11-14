using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public class ObjManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private Events _Logging;
        private TopologyManager _Topology;
        private Node _Node;
        private UserManager _Users;

        #endregion

        #region Constructors-and-Factories

        public ObjManager(Settings settings, Events logging, TopologyManager topology, Node node, UserManager users)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (topology == null) throw new ArgumentNullException(nameof(topology));
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (users == null) throw new ArgumentNullException(nameof(users));

            _Settings = settings;
            _Logging = logging;
            _Topology = topology;
            _Node = node;
            _Users = users;
        }

        #endregion

        #region Public-Methods

        public Obj Build(RequestMetadata md)
        {
            #region Check-for-Null-Values

            if (md == null) throw new ArgumentNullException(nameof(md));
            if (md.CurrHttpReq == null) throw new ArgumentException("CurrentHttpRequest is null");
            if (md.CurrUser == null) throw new ArgumentException("CurrentUserMaster is null");
            if (_Users == null) throw new ArgumentNullException(nameof(_Users));
            if (_Topology == null) throw new ArgumentNullException(nameof(_Topology));
            if (_Node == null) throw new ArgumentNullException(nameof(_Node));
            if (_Logging == null) throw new ArgumentNullException(nameof(_Logging));

            #endregion

            #region Base-Parameters

            Obj ret = new Obj(); 

            int rueCount = 1;
            int expSec = 0;
            string tags = "";
            bool gatewayMode = false;

            if (Common.IsTrue(md.CurrHttpReq.RetrieveHeaderValue("container")))
            {
                ret.IsContainer = 1;
                ret.IsObject = 0;
            }
            else
            {
                ret.IsContainer = 0;
                ret.IsObject = 1;
            }

            ret.ReplicationMode = md.CurrHttpReq.RetrieveHeaderValue("replication_mode");
            if (String.IsNullOrEmpty(ret.ReplicationMode)) ret.ReplicationMode = _Settings.Replication.ReplicationMode;

            switch (ret.ReplicationMode)
            {
                case "none":
                    ret.Replicas = null;
                    break;

                case "sync":
                case "async":
                    ret.Replicas = _Topology.GetReplicas();
                    break;

                default:
                    _Logging.Log(LoggingModule.Severity.Warn, "BuildObj invalid replication mode set in querystring: " + ret.ReplicationMode);
                    return null;
            }

            ret.IsCompressed = 0;
            ret.IsEncrypted = 0;
            ret.IsEncoded = 0;

            if (md.CurrUser.GetCompressionMode(_Settings)) ret.IsCompressed = 1;
            if (md.CurrUser.GetEncryptionMode(_Settings)) ret.IsEncrypted = 1;

            if (Common.IsTrue(ret.IsObject))
            {
                if (Common.IsTrue(md.CurrHttpReq.RetrieveHeaderValue("compress")))
                {
                    ret.IsCompressed = 1;
                }

                if (Common.IsTrue(md.CurrHttpReq.RetrieveHeaderValue("encrypt")))
                {
                    ret.IsEncrypted = 1;
                }

                if (Common.IsTrue(md.CurrHttpReq.RetrieveHeaderValue("encoded")))
                {
                    ret.IsEncoded = 1;
                }
            }

            ret.ContentType = md.CurrHttpReq.RetrieveHeaderValue("content-type");
            tags = md.CurrHttpReq.RetrieveHeaderValue("x-tags");
            ret.Tags = Common.CsvToStringList(tags);
            ret.Value = md.CurrHttpReq.Data;

            gatewayMode = md.CurrUser.GetGatewayMode(_Settings);
            if (gatewayMode) ret.GatewayMode = 1;
            else ret.GatewayMode = 0;

            if (Common.IsTrue(ret.GatewayMode))
            {
                ret.IsCompressed = 0;
                ret.IsEncrypted = 0;
                ret.IsEncoded = 0;
            }

            expSec = md.CurrUser.GetExpirationSeconds(_Settings, md.CurrApiKey);
            if (expSec > 0)
            {
                ret.Expiration = DateTime.Now.AddSeconds(expSec);
            }

            #endregion

            #region Set-Container-and-Object-Data

            foreach (string currRue in md.CurrHttpReq.RawUrlEntries)
            {
                if (String.IsNullOrEmpty(currRue)) continue;

                if (rueCount == 1)
                {
                    // logging.Log(LoggingModule.Severity.Debug, "BuildObj adding currRue " + currRue + " as currObj.UserGuid");
                    ret.UserGuid = String.Copy(currRue);
                    rueCount++;
                    continue;
                }

                if (Common.IsTrue(ret.IsContainer))
                {
                    // logging.Log(LoggingModule.Severity.Debug, "BuildObj adding currRue " + currRue + " to ContainerPath");
                    ret.ContainerPath.Add(currRue);
                    rueCount++;
                    continue;
                }

                if (Common.IsTrue(ret.IsObject))
                {
                    if (rueCount == md.CurrHttpReq.RawUrlEntries.Count)
                    {
                        // logging.Log(LoggingModule.Severity.Debug, "BuildObj adding " + currRue + " to Key");
                        ret.Key = currRue;
                        rueCount++;
                        break;
                    }
                    else
                    {
                        // logging.Log(LoggingModule.Severity.Debug, "BuildObj adding " + currRue + " to ContainerPath (in IsObject)");
                        ret.ContainerPath.Add(currRue);
                        rueCount++;
                        continue;
                    }
                }
            }

            if (String.IsNullOrEmpty(ret.UserGuid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildObj unable to find UserGuid in URL");
                return null;
            }

            #endregion

            #region Determine-Primary-and-URLs

            ret.PrimaryNode = _Topology.DetermineOwner(md.CurrUser.Guid);
            if (ret.PrimaryNode == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildObj unable to determine primary for user GUID " + md.CurrUser.Guid);
                return null;
            }

            ret.PrimaryUrlWithQs = PrimaryUrl(true, md.CurrHttpReq, ret);
            if (String.IsNullOrEmpty(ret.PrimaryUrlWithQs))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildObj unable to build primary URL for request (with querystring)");
                return null;
            }

            ret.PrimaryUrlWithoutQs = PrimaryUrl(false, null, ret);
            if (String.IsNullOrEmpty(ret.PrimaryUrlWithoutQs))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildObj unable to build primary URL for request (without querystring)");
                return null;
            }

            #endregion

            #region Build-Disk-Path

            ret.DiskPath = DiskPath(ret, md.CurrUser);
            if (String.IsNullOrEmpty(ret.DiskPath))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildObj unable to build disk path for obj");
                return null;
            }

            #endregion

            #region Calculate-MD5

            if (ret.Value != null)
            {
                if (ret.Value.Length > 0)
                {
                    ret.Md5Hash = Common.Md5(ret.Value);
                }
            }

            #endregion

            return ret;
        }

        public Obj BuildFromDisk(string path)
        {
            #region Check-for-Null-Values

            if (_Users == null) throw new ArgumentNullException(nameof(_Users));
            if (_Topology == null) throw new ArgumentNullException(nameof(_Topology));
            if (_Node == null) throw new ArgumentNullException(nameof(_Node));
            if (_Logging == null) throw new ArgumentNullException(nameof(_Logging));

            #endregion

            #region Variables

            Obj ret = new Obj(); 

            string directoryName = "";
            string filenameWithExtension = "";
            string filenameWithoutExtension = "";
            string fileExtension = "";

            UserMaster currUser = new UserMaster();
            string userHomeDirectory = "";
            bool isGlobal = false;
            bool userGatewayMode = false;

            ObjInfo currObjInfo = new ObjInfo();
            string currObjJson = "";
            List<string> containers = new List<string>();
            string key = "";

            #endregion

            #region Retrieve-User-Details

            currUser = UserMaster.FromPath(path, _Settings, _Users, out userHomeDirectory, out isGlobal);
            if (currUser == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk unable to determine user and home directory from supplied disk path " + path);
                return null;
            }
            else
            {
                if (isGlobal)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "BuildObjFromDisk detected use of global home directory " + userHomeDirectory);
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "BuildObjFromDisk detected user GUID " + currUser.Guid + " home directory " + userHomeDirectory);
                }
            }

            #endregion

            #region Retrieve-Object-Metadata

            currObjInfo = ObjInfo.FromFile(path);
            if (currObjInfo == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk null file info returned for " + path);
                return null;
            }

            #endregion

            #region Parse-Path

            directoryName = Path.GetDirectoryName(path) + Common.GetPathSeparator(_Settings.Environment);
            filenameWithExtension = Path.GetFileName(path);
            filenameWithoutExtension = Path.GetFileNameWithoutExtension(path);
            fileExtension = Path.GetExtension(path);

            #endregion

            #region Read-Object

            userGatewayMode = currUser.GetGatewayMode(_Settings);
            if (!Common.IsTrue(userGatewayMode))
            {
                #region Read-and-Deserialize

                currObjJson = Common.ReadTextFile(path);
                if (String.IsNullOrEmpty(currObjJson))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk empty file string returned when reading " + path);
                    return null;
                }

                try
                {
                    ret = Common.DeserializeJson<Obj>(currObjJson);
                    if (ret == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk null obj after deserializing " + path);
                        return null;
                    }
                }
                catch (Exception)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk unable to deserialize current object");
                    return null;
                }

                #endregion
            }
            else
            {
                #region Read-Object-Contents

                ret.Value = Common.ReadBinaryFile(path);
                if (ret.Value == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk empty file returned when reading " + path);
                    return null;
                }

                #endregion

                #region Set-Static-Values

                ret.IsCompressed = 0;
                ret.IsEncrypted = 0;
                ret.IsEncoded = 0;
                ret.IsContainer = 0;
                ret.IsObject = 1;
                ret.ReplicationMode = _Settings.Replication.ReplicationMode;
                ret.PrimaryNode = _Node;
                ret.Replicas = _Topology.GetReplicas();
                ret.GatewayMode = 1;
                ret.DiskPath = path;

                if (ret.Value != null)
                {
                    if (ret.Value.Length > 0)
                    {
                        ret.Md5Hash = Common.Md5(ret.Value);
                    }
                }

                if (!GetKeyGuidContainers(
                    path,
                    userHomeDirectory,
                    false, 
                    out containers,
                    out key))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk unable to retrieve key, GUID, and containers for " + path);
                    return null;
                }

                ret.ContainerPath = containers;
                ret.UserGuid = currUser.Guid;
                ret.Key = key;
                ret.ContentType = MimeTypes.GetFromExtension(Common.GetFileExtension(ret.DiskPath));
                ret.PrimaryUrlWithQs = PrimaryUrl(true, null, ret);
                ret.PrimaryUrlWithoutQs = PrimaryUrl(false, null, ret);

                #endregion
            }

            #endregion

            return ret;
        }

        public string PrimaryUrl(bool includeQuery, HttpRequest req, Obj currObj)
        {
            #region Check-for-Null-Values

            if (currObj == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildPrimaryUrlWithQuery null path object supplied");
                return null;
            }

            if (currObj.PrimaryNode == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildPrimaryUrlWithQuery null path primary object supplied");
                return null;
            }

            #endregion

            #region Variables

            string url = "";

            #endregion

            #region Process

            if (Common.IsTrue(currObj.PrimaryNode.Ssl)) url = "https://";
            else url = "http://";
            url += currObj.PrimaryNode.DnsHostname + ":" + currObj.PrimaryNode.Port + "/";
            url += currObj.UserGuid + "/";

            if (currObj.ContainerPath != null)
            {
                foreach (string currContainer in currObj.ContainerPath)
                {
                    if (String.IsNullOrEmpty(currContainer)) continue;
                    url += currContainer;
                    if (!currContainer.EndsWith("/")) url += "/";
                }
            }

            if (!String.IsNullOrEmpty(currObj.Key)) url += currObj.Key;

            if (includeQuery)
            {
                if (req != null)
                {
                    if (req.QuerystringEntries != null && req.QuerystringEntries.Count > 0)
                    {
                        url += "?";
                        int addedCount = 0;

                        foreach (KeyValuePair<string, string> currQse in req.QuerystringEntries)
                        {
                            if (addedCount == 0)
                            {
                                url += currQse.Key + "=" + currQse.Value;
                                addedCount++;
                            }
                            else
                            {
                                url += "&" + currQse.Key + "=" + currQse.Value;
                                addedCount++;
                            }
                        }
                    }
                }
            }

            return url;

            #endregion
        }

        public string PrimaryUrlFromDisk(string filename, Obj obj)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(filename))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildUrlFromFilePath null filename supplied");
                return null;
            }

            if (_Node == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildUrlFromFilePath null node supplied");
                return null;
            }

            #endregion

            #region Variables

            string separator = Common.GetPathSeparator(_Settings.Environment);
            string homeDirectory = "";
            string tempStr = "";
            string url = "";
            List<string> relativePath = new List<string>();

            #endregion

            #region Get-User-Home-Directory

            homeDirectory = _Users.GetHomeDirectory(obj.UserGuid, _Settings);
            if (String.IsNullOrEmpty(homeDirectory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildUrlFromFilePath unable to retrieve home directory for user GUID " + obj.UserGuid);
                return null;
            }

            #endregion

            #region Relative-Path

            filename = filename.Replace(homeDirectory, "");

            tempStr = "";
            for (int i = 0; i < filename.Length; i++)
            {
                if (String.Compare(filename[i].ToString(), separator) == 0)
                {
                    if (!String.IsNullOrEmpty(tempStr))
                    {
                        relativePath.Add(tempStr);
                        tempStr = "";
                        continue;
                    }
                }
                else
                {
                    tempStr += filename[i];
                }
            }

            if (!String.IsNullOrEmpty(tempStr))
            {
                relativePath.Add(tempStr);
                tempStr = "";
            }

            // EventHandler.Log(LoggingModule.Severity.Debug, " relative path: " + relativePath.Count + " entries");

            #endregion

            #region Process-URL

            if (Common.IsTrue(_Node.Ssl)) url += "https://";
            else url += "http://";

            url += _Node.DnsHostname + ":" + _Node.Port;
            // EventHandler.Log(LoggingModule.Severity.Debug, " url original: " + url);

            foreach (string currStr in relativePath)
            {
                url += "/" + currStr;
                // EventHandler.Log(LoggingModule.Severity.Debug, " url amended: " + url);
            }

            // EventHandler.Log(LoggingModule.Severity.Debug, " url final: " + url);

            #endregion

            #region Process-Querystring

            url += "?proxied=true";

            if (obj != null)
            {
                if (Common.IsTrue(obj.IsEncrypted)) url += "&encrypt=true";
                if (Common.IsTrue(obj.IsCompressed)) url += "&compress=true";
                if (!String.IsNullOrEmpty(obj.ReplicationMode)) url += "&replication_mode=" + obj.ReplicationMode;
            }

            #endregion

            return url;
        }

        public List<string> ReplicaUrls(bool includeQuery, HttpRequest req, Obj currObj)
        {
            #region Check-for-Null-Values

            if (currObj == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildReplicaUrlsWithQuery null path object supplied");
                return null;
            }

            if (currObj.PrimaryNode == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildReplicaUrlsWithQuery null path primary object supplied");
                return null;
            }

            if (currObj.PrimaryNode.Neighbors == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildReplicaUrlsWithQuery null neighbors list in primary");
                return null;
            }

            if (currObj.PrimaryNode.Neighbors.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildReplicaUrlsWithQuery empty neighbors list in primary");
                return null;
            }

            #endregion

            #region Variables

            List<string> urlList = new List<string>();
            string url = "";

            #endregion

            #region Build-URLs

            foreach (int currNodeId in currObj.PrimaryNode.Neighbors)
            {
                // retrieve the node
                foreach (Node currNode in _Topology.GetReplicas())
                {
                    if (currNodeId == currNode.NodeId
                        || currObj.PrimaryNode.NodeId == currNode.NodeId)
                    {
                        url = "";
                        if (Common.IsTrue(currNode.Ssl)) url = "https://";
                        else url = "http://";
                        url += currNode.DnsHostname + ":" + currNode.Port + "/";
                        url += currObj.UserGuid + "/";

                        if (currObj.ContainerPath != null)
                        {
                            foreach (string currContainer in currObj.ContainerPath)
                            {
                                if (String.IsNullOrEmpty(currContainer)) continue;
                                url += currContainer;
                                if (!currContainer.EndsWith("/")) url += "/";
                            }
                        }

                        if (!String.IsNullOrEmpty(currObj.Key)) url += currObj.Key;
                        url += "?proxied=true";

                        if (includeQuery)
                        {
                            if (req.QuerystringEntries != null && req.QuerystringEntries.Count > 0)
                            {
                                foreach (KeyValuePair<string, string> currQse in req.QuerystringEntries)
                                {
                                    url += "&" + currQse.Key + "=" + currQse.Value;
                                }
                            }
                        }

                        urlList.Add(url);
                    }
                }
            }

            #endregion

            urlList = urlList.Distinct().ToList();
            return urlList;
        }

        public List<string> MaintenanceUrls(bool includeQuery, HttpRequest req, Obj currObj)
        {
            #region Check-for-Null-Values

            if (currObj == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildMaintReadReplicaUrlsWithQuery null path object supplied");
                return null;
            }

            if (currObj.PrimaryNode == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildMaintReadReplicaUrlsWithQuery null path primary object supplied");
                return null;
            }

            List<Node> nodes = _Topology.GetNodes();
            if (nodes == null || nodes.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildMaintReadReplicaUrlsWithQuery null node or empty list in topology");
                return null;
            }
            
            #endregion

            #region Variables

            List<string> urlList = new List<string>();
            string url = "";

            #endregion

            #region Build-URLs

            foreach (Node currNode in nodes)
            {
                url = "";
                if (Common.IsTrue(currNode.Ssl)) url = "https://";
                else url = "http://";
                url += currNode.DnsHostname + ":" + currNode.Port + "/";
                url += currObj.UserGuid + "/";

                if (currObj.ContainerPath != null)
                {
                    foreach (string currContainer in currObj.ContainerPath)
                    {
                        if (String.IsNullOrEmpty(currContainer)) continue;
                        url += currContainer;
                        if (!currContainer.EndsWith("/")) url += "/";
                    }
                }

                if (!String.IsNullOrEmpty(currObj.Key)) url += currObj.Key;
                url += "?proxied=true";

                if (includeQuery)
                {
                    if (req.QuerystringEntries != null && req.QuerystringEntries.Count > 0)
                    {
                        foreach (KeyValuePair<string, string> currQse in req.QuerystringEntries)
                        {
                            url += "&" + currQse.Key + "=" + currQse.Value;
                        }
                    }
                }

                urlList.Add(url);
            }

            #endregion

            urlList = urlList.Distinct().ToList();
            return urlList;
        }

        public string RedirectUrl(bool includeQuery, HttpRequest req, Obj currObj)
        {
            #region Check-for-Null-Values

            if (currObj == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildRedirectUrlWithQuery null path object supplied");
                return null;
            }

            if (currObj.PrimaryNode == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildRedirectUrlWithQuery null path primary object supplied");
                return null;
            }

            #endregion

            #region Variables

            string url = "";
            Node availableNode = null;

            #endregion

            #region Process

            #region Check-for-Owner-First

            foreach (Node currNode in _Topology.GetNodes())
            {
                if (currNode.NodeId == currObj.PrimaryNode.NodeId)
                {
                    if (currNode.NumFailures == 0)
                    {
                        availableNode = currNode;
                        break;
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "BuildRedirectUrlWithQuery owning NodeId " + currNode.NodeId + " is unavailable (failure count greater than zero), searching for peer");
                        break;
                    }
                }
            }

            #endregion

            #region Check-for-Neighbors-if-Needed

            if (availableNode == null)
            {
                if (currObj.PrimaryNode.Neighbors != null)
                {
                    foreach (Node currNode in _Topology.GetNodes())
                    {
                        if (currObj.PrimaryNode.Neighbors.Contains(currNode.NodeId))
                        {
                            if (currNode.NumFailures == 0)
                            {
                                availableNode = currNode;
                                break;
                            }
                            else
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "BuildRedirectUrlWithQuery neighbor NodeId " + currNode.NodeId + " is unavailable (failure count greater than zero), searching for peer");
                                break;
                            }
                        }
                    }
                }
            }

            #endregion

            #region Build-and-Return-URL

            if (availableNode != null)
            {
                url = "";
                if (Common.IsTrue(availableNode.Ssl)) url = "https://";
                else url = "http://";
                url += availableNode.DnsHostname + ":" + availableNode.Port + "/";
                url += currObj.UserGuid + "/";

                if (currObj.ContainerPath != null)
                {
                    foreach (string currContainer in currObj.ContainerPath)
                    {
                        if (String.IsNullOrEmpty(currContainer)) continue;
                        url += currContainer;
                        if (!currContainer.EndsWith("/")) url += "/";
                    }
                }

                if (!String.IsNullOrEmpty(currObj.Key)) url += currObj.Key;
                url += "?redirected=true";

                if (includeQuery)
                {
                    if (req.QuerystringEntries != null && req.QuerystringEntries.Count > 0)
                    {
                        foreach (KeyValuePair<string, string> currQse in req.QuerystringEntries)
                        {
                            url += "&" + currQse.Key + "=" + currQse.Value;
                        }
                    }
                }

                return url;
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildRedirectUrlWithQuery no available neighbor found");
                return null;
            }

            #endregion

            #endregion
        }

        public string DiskPath(Obj currObj, UserMaster currUser)
        {
            #region Check-for-Null-Values

            if (currObj == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath null object supplied");
                return null;
            }

            if (currUser == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath null user supplied");
                return null;
            }

            #endregion

            #region Variables

            string homeDirectory = "";
            string fullPath = "";

            #endregion

            #region Get-Home-Directory

            if (String.IsNullOrEmpty(currUser.HomeDirectory))
            {
                // global directory
                homeDirectory = String.Copy(_Settings.Storage.Directory);
                if (!homeDirectory.EndsWith(Common.GetPathSeparator(_Settings.Environment))) homeDirectory += Common.GetPathSeparator(_Settings.Environment);
                homeDirectory += currUser.Guid;
                homeDirectory += Common.GetPathSeparator(_Settings.Environment);
            }
            else
            {
                // user-specific home directory
                homeDirectory = String.Copy(currUser.HomeDirectory);
                if (!homeDirectory.EndsWith(Common.GetPathSeparator(_Settings.Environment))) homeDirectory += Common.GetPathSeparator(_Settings.Environment);
            }

            #endregion

            #region Process

            fullPath = String.Copy(homeDirectory);

            if (currObj.ContainerPath != null)
            {
                if (currObj.ContainerPath.Count > 0)
                {
                    foreach (string currContainer in currObj.ContainerPath)
                    {
                        if (String.IsNullOrEmpty(currContainer)) continue;

                        if (Common.ContainsUnsafeCharacters(currContainer))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath unsafe characters detected: " + currContainer);
                            return null;
                        }

                        fullPath += currContainer + Common.GetPathSeparator(_Settings.Environment);
                    }
                }
            }

            if (!String.IsNullOrEmpty(currObj.Key))
            {
                fullPath += currObj.Key;
            }

            return fullPath;

            #endregion
        }

        #endregion

        #region Private-Methods

        private bool GetKeyGuidContainers(
            string path,
            string homeDirectory,
            bool isContainer, 
            out List<string> containers,
            out string key)
        {
            containers = new List<string>();
            key = "";

            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(path))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "GetKeyGuidContainers null path supplied");
                return false;
            }

            #endregion

            #region Variables

            string tempString = "";
            string reduced = "";

            #endregion

            #region Remove-Storage-Directory-from-Path

            reduced = path.Replace(homeDirectory + Common.GetPathSeparator(_Settings.Environment), "");
            reduced = reduced.Replace(homeDirectory, "");

            #endregion

            #region Containers

            foreach (char c in reduced)
            {
                if (String.Compare(c.ToString(), Common.GetPathSeparator(_Settings.Environment)) == 0)
                {
                    // EventHandler.Log(LoggingModule.Severity.Debug, "GetKeyGuidContainers encountered path separator: " + Common.GetPathSeparator(settings.Environment));

                    if (!String.IsNullOrEmpty(tempString))
                    {
                        if (String.Compare(tempString, Common.GetPathSeparator(_Settings.Environment)) == 0)
                        {
                            // EventHandler.Log(LoggingModule.Severity.Debug, "GetKeyGuidContainers encountered path separator in temp string, skipping");
                            tempString = "";
                            continue;
                        }

                        // EventHandler.Log(LoggingModule.Severity.Debug, "GetKeyGuidContainers adding " + tempString + " to containerPath");
                        containers.Add(tempString);
                        tempString = "";
                        continue;
                    }
                }

                tempString += c;
            }

            // EventHandler.Log(LoggingModule.Severity.Debug, "GetKeyGuidContainers exiting iterator, tempString is " + tempString);
            if (!String.IsNullOrEmpty(tempString))
            {
                // EventHandler.Log(LoggingModule.Severity.Debug, "GetKeyGuidContainers tempString is not null, adding: " + tempString);
                containers.Add(tempString);
                tempString = "";
            }

            #endregion

            #region Extract-Key

            if (!isContainer)
            {
                foreach (string currContainer in containers)
                {
                    key = currContainer;
                }

                containers.RemoveAt(containers.Count - 1);
            }

            #endregion

            return true;
        }

        #endregion
    }
}
