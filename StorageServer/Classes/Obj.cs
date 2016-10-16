using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public class Obj
    {
        #region Public-Members

        public string UserGuid { get; set; }
        public string Key { get; set; }
        public Node PrimaryNode { get; set; }
        public string PrimaryUrlWithQs { get; set; }
        public string PrimaryUrlWithoutQs { get; set; }
        public string ReplicationMode { get; set; }
        public List<Node> Replicas { get; set; }
        public string ContentType { get; set; }
        public DateTime? Created { get; set; }
        public DateTime? LastUpdate { get; set; }
        public DateTime? LastAccess { get; set; }
        public DateTime? Expiration { get; set; }
        public int? IsCompressed { get; set; }
        public int? IsEncrypted { get; set; }
        public int? IsEncoded { get; set; }
        public string EncryptionKsn { get; set; }
        public int? IsContainer { get; set; }
        public int? IsObject { get; set; }
        public int? GatewayMode { get; set; }
        public List<string> ContainerPath { get; set; }
        public List<string> Tags { get; set; }
        public string DiskPath { get; set; }
        public byte[] Value { get; set; }
        public string Md5Hash { get; set; }

        #endregion

        #region Constructors-and-Factories

        public Obj()
        {

        }

        public static Obj BuildObj(RequestMetadata md, UserManager users, Settings settings, Topology topology, Node node, Events logging)
        {
            #region Check-for-Null-Values

            if (md == null) throw new ArgumentNullException(nameof(md));
            if (md.CurrentHttpRequest == null) throw new ArgumentException("CurrentHttpRequest is null");
            if (md.CurrentUserMaster == null) throw new ArgumentException("CurrentUserMaster is null");
            if (users == null) throw new ArgumentNullException(nameof(users));
            if (topology == null) throw new ArgumentNullException(nameof(topology));
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            #endregion

            #region Base-Parameters

            Obj ret = new Obj();
            ret.ContainerPath = new List<string>();

            int rueCount = 1;
            int expSec = 0;
            string tags = "";
            bool gatewayMode = false;

            if (Common.IsTrue(md.CurrentHttpRequest.RetrieveHeaderValue("container")))
            {
                ret.IsContainer = 1;
                ret.IsObject = 0;
            }
            else
            {
                ret.IsContainer = 0;
                ret.IsObject = 1;
            }

            ret.ReplicationMode = md.CurrentHttpRequest.RetrieveHeaderValue("replication_mode");
            if (String.IsNullOrEmpty(ret.ReplicationMode)) ret.ReplicationMode = settings.Replication.ReplicationMode;

            switch (ret.ReplicationMode)
            {
                case "none":
                    ret.Replicas = null;
                    break;

                case "sync":
                case "async":
                    ret.Replicas = topology.Replicas;
                    break;

                default:
                    logging.Log(LoggingModule.Severity.Warn, "BuildObj invalid replication mode set in querystring: " + ret.ReplicationMode);
                    return null;
            }

            ret.IsCompressed = 0;
            ret.IsEncrypted = 0;
            ret.IsEncoded = 0;

            if (md.CurrentUserMaster.GetCompressionMode(settings)) ret.IsCompressed = 1;
            if (md.CurrentUserMaster.GetEncryptionMode(settings)) ret.IsEncrypted = 1;

            if (Common.IsTrue(ret.IsObject))
            {
                if (Common.IsTrue(md.CurrentHttpRequest.RetrieveHeaderValue("compress")))
                {
                    ret.IsCompressed = 1;
                }

                if (Common.IsTrue(md.CurrentHttpRequest.RetrieveHeaderValue("encrypt")))
                {
                    ret.IsEncrypted = 1;
                }

                if (Common.IsTrue(md.CurrentHttpRequest.RetrieveHeaderValue("encoded")))
                {
                    ret.IsEncoded = 1;
                }
            }

            ret.ContentType = md.CurrentHttpRequest.RetrieveHeaderValue("content-type");
            tags = md.CurrentHttpRequest.RetrieveHeaderValue("x-tags");
            ret.Tags = Common.CsvToStringList(tags);
            ret.Value = md.CurrentHttpRequest.Data;

            gatewayMode = md.CurrentUserMaster.GetGatewayMode(settings);
            if (gatewayMode) ret.GatewayMode = 1;
            else ret.GatewayMode = 0;

            if (Common.IsTrue(ret.GatewayMode))
            {
                ret.IsCompressed = 0;
                ret.IsEncrypted = 0;
                ret.IsEncoded = 0;
            }

            expSec = md.CurrentUserMaster.GetExpirationSeconds(settings, md.CurrentApiKey);
            if (expSec > 0)
            {
                ret.Expiration = DateTime.Now.AddSeconds(expSec);
            }

            #endregion

            #region Set-Container-and-Object-Data

            foreach (string currRue in md.CurrentHttpRequest.RawUrlEntries)
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
                    if (rueCount == md.CurrentHttpRequest.RawUrlEntries.Count)
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
                logging.Log(LoggingModule.Severity.Warn, "BuildObj unable to find UserGuid in URL");
                return null;
            }

            #endregion

            #region Determine-Primary-and-URLs

            ret.PrimaryNode = Node.DetermineOwner(md.CurrentUserMaster.Guid, users, topology, node, logging);
            if (ret.PrimaryNode == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildObj unable to determine primary for user GUID " + md.CurrentUserMaster.Guid);
                return null;
            }

            ret.PrimaryUrlWithQs = BuildPrimaryUrl(true, md.CurrentHttpRequest, ret, logging);
            if (String.IsNullOrEmpty(ret.PrimaryUrlWithQs))
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildObj unable to build primary URL for request (with querystring)");
                return null;
            }

            ret.PrimaryUrlWithoutQs = BuildPrimaryUrl(false, null, ret, logging);
            if (String.IsNullOrEmpty(ret.PrimaryUrlWithoutQs))
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildObj unable to build primary URL for request (without querystring)");
                return null;
            }

            #endregion

            #region Build-Disk-Path

            ret.DiskPath = Obj.BuildDiskPath(ret, md.CurrentUserMaster, settings, logging);
            if (String.IsNullOrEmpty(ret.DiskPath))
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildObj unable to build disk path for obj");
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

        public static Obj BuildObjFromDisk(string path, UserManager users, Settings settings, Topology topology, Node node, Events logging)
        {
            #region Check-for-Null-Values

            if (users == null) throw new ArgumentNullException(nameof(users));
            if (topology == null) throw new ArgumentNullException(nameof(topology));
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            #endregion

            #region Variables

            Obj ret = new Obj();
            ret.ContainerPath = new List<string>();

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

            currUser = UserMaster.FromPath(path, settings, users, out userHomeDirectory, out isGlobal);
            if (currUser == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk unable to determine user and home directory from supplied disk path " + path);
                return null;
            }
            else
            {
                if (isGlobal)
                {
                    logging.Log(LoggingModule.Severity.Debug, "BuildObjFromDisk detected use of global home directory " + userHomeDirectory);
                }
                else
                {
                    logging.Log(LoggingModule.Severity.Debug, "BuildObjFromDisk detected user GUID " + currUser.Guid + " home directory " + userHomeDirectory);
                }
            }

            #endregion

            #region Retrieve-Object-Metadata

            currObjInfo = ObjInfo.FromFile(path);
            if (currObjInfo == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk null file info returned for " + path);
                return null;
            }

            #endregion

            #region Parse-Path

            directoryName = Path.GetDirectoryName(path) + Common.GetPathSeparator(settings.Environment);
            filenameWithExtension = Path.GetFileName(path);
            filenameWithoutExtension = Path.GetFileNameWithoutExtension(path);
            fileExtension = Path.GetExtension(path);

            #endregion

            #region Read-Object

            userGatewayMode = currUser.GetGatewayMode(settings);
            if (!Common.IsTrue(userGatewayMode))
            {
                #region Read-and-Deserialize

                currObjJson = Common.ReadTextFile(path);
                if (String.IsNullOrEmpty(currObjJson))
                {
                    logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk empty file string returned when reading " + path);
                    return null;
                }

                try
                {
                    ret = Common.DeserializeJson<Obj>(currObjJson);
                    if (ret == null)
                    {
                        logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk null obj after deserializing " + path);
                        return null;
                    }
                }
                catch (Exception)
                {
                    logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk unable to deserialize current object");
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
                    logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk empty file returned when reading " + path);
                    return null;
                }

                #endregion

                #region Set-Static-Values

                ret.IsCompressed = 0;
                ret.IsEncrypted = 0;
                ret.IsEncoded = 0;
                ret.IsContainer = 0;
                ret.IsObject = 1;
                ret.ReplicationMode = settings.Replication.ReplicationMode;
                ret.PrimaryNode = node;
                ret.Replicas = topology.Replicas;
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
                    settings,
                    logging,
                    out containers,
                    out key))
                {
                    logging.Log(LoggingModule.Severity.Warn, "BuildObjFromDisk unable to retrieve key, GUID, and containers for " + path);
                    return null;
                }

                ret.ContainerPath = containers;
                ret.UserGuid = currUser.Guid;
                ret.Key = key;
                ret.ContentType = MimeTypes.GetFromExtension(Common.GetFileExtension(ret.DiskPath));
                ret.PrimaryUrlWithQs = Obj.BuildPrimaryUrl(true, null, ret, logging);
                ret.PrimaryUrlWithoutQs = Obj.BuildPrimaryUrl(false, null, ret, logging);

                #endregion
            }

            #endregion

            return ret;
        }

        #endregion

        #region Private-Methods

        private static bool GetKeyGuidContainers(
            string path,
            string homeDirectory,
            bool isContainer,
            Settings settings,
            Events logging,
            out List<string> containers,
            out string key)
        {
            containers = new List<string>();
            key = "";
            
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(path))
            {
                logging.Log(LoggingModule.Severity.Warn, "GetKeyGuidContainers null path supplied");
                return false;
            }

            #endregion

            #region Variables

            string tempString = "";
            string reduced = "";

            #endregion

            #region Remove-Storage-Directory-from-Path

            reduced = path.Replace(homeDirectory + Common.GetPathSeparator(settings.Environment), "");
            reduced = reduced.Replace(homeDirectory, "");

            #endregion
            
            #region Containers

            foreach (char c in reduced)
            {
                if (String.Compare(c.ToString(), Common.GetPathSeparator(settings.Environment)) == 0)
                {
                    // EventHandler.Log(LoggingModule.Severity.Debug, "GetKeyGuidContainers encountered path separator: " + Common.GetPathSeparator(settings.Environment));

                    if (!String.IsNullOrEmpty(tempString))
                    {
                        if (String.Compare(tempString, Common.GetPathSeparator(settings.Environment)) == 0)
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

        #region Public-Methods

        public override string ToString()
        {
            string ret = "";

            ret += "Key " + Key + " User GUID " + UserGuid + Environment.NewLine;
            ret += "  Primary Node: " + PrimaryNode.ToString();
            ret += "  Primary URL with Query    : " + PrimaryUrlWithQs + Environment.NewLine;
            ret += "  Primary URL without Query : " + PrimaryUrlWithoutQs + Environment.NewLine;

            if (Replicas != null)
            {
                ret += "  Replicas   : " + Environment.NewLine;
                foreach (Node curr in Replicas) ret += "  " + curr.ToString();
            }
            else
            {
                ret += "  Replicas: none" + Environment.NewLine;
            }

            if (IsCompressed != null)   ret += "  Compressed : " + IsCompressed + Environment.NewLine;
            if (IsEncrypted != null) ret += "  Encrypted  : " + IsEncrypted + Environment.NewLine;
            if (IsEncoded != null) ret += "  Encoded    : " + IsEncoded + Environment.NewLine;
            ret += "  Disk Path  : " + DiskPath + Environment.NewLine;
            return ret;
        }

        #endregion

        #region Public-Static-Methods

        public static string BuildPrimaryUrl(bool includeQuery, HttpRequest req, Obj currObj, Events logging)
        {
            #region Check-for-Null-Values

            if (currObj == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildPrimaryUrlWithQuery null path object supplied");
                return null;
            }

            if (currObj.PrimaryNode == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildPrimaryUrlWithQuery null path primary object supplied");
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
        
        public static string BuildUrlFromFilePath(string filename, Node node, Obj obj, UserManager users, Settings settings, Events logging)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(filename))
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildUrlFromFilePath null filename supplied");
                return null;
            }

            if (node == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildUrlFromFilePath null node supplied");
                return null;
            }

            #endregion

            #region Variables

            string separator = Common.GetPathSeparator(settings.Environment);
            string homeDirectory = "";
            string tempStr = "";
            string url = "";
            List<string> relativePath = new List<string>();

            #endregion

            #region Get-User-Home-Directory

            homeDirectory = users.GetHomeDirectory(obj.UserGuid, settings);
            if (String.IsNullOrEmpty(homeDirectory))
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildUrlFromFilePath unable to retrieve home directory for user GUID " + obj.UserGuid);
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

            if (Common.IsTrue(node.Ssl)) url += "https://";
            else url += "http://";

            url += node.DnsHostname + ":" + node.Port;
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

        public static List<string> BuildReplicaUrls(bool includeQuery, HttpRequest req, Obj currObj, Topology topology, Events logging)
        {
            #region Check-for-Null-Values

            if (currObj == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildReplicaUrlsWithQuery null path object supplied");
                return null;
            }

            if (currObj.PrimaryNode == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildReplicaUrlsWithQuery null path primary object supplied");
                return null;
            }

            if (currObj.PrimaryNode.Neighbors == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildReplicaUrlsWithQuery null neighbors list in primary");
                return null;
            }

            if (currObj.PrimaryNode.Neighbors.Count < 1)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildReplicaUrlsWithQuery empty neighbors list in primary");
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
                foreach (Node currNode in topology.Nodes)
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
        
        public static List<string> BuildMaintReadUrls(bool includeQuery, HttpRequest req, Obj currObj, Topology topology, Events logging)
        {
            #region Check-for-Null-Values

            if (currObj == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildMaintReadReplicaUrlsWithQuery null path object supplied");
                return null;
            }

            if (currObj.PrimaryNode == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildMaintReadReplicaUrlsWithQuery null path primary object supplied");
                return null;
            }

            if (topology.Nodes == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildMaintReadReplicaUrlsWithQuery null node list in topology");
                return null;
            }

            if (topology.Nodes.Count < 1)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildMaintReadReplicaUrlsWithQuery null node list in topology");
                return null;
            }

            #endregion

            #region Variables

            List<string> urlList = new List<string>();
            string url = "";

            #endregion

            #region Build-URLs

            foreach (Node currNode in topology.Nodes)
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
        
        public static string BuildRedirectUrl(bool includeQuery, HttpRequest req, Obj currObj, Topology topology, Events logging)
        {
            #region Check-for-Null-Values

            if (currObj == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildRedirectUrlWithQuery null path object supplied");
                return null;
            }

            if (currObj.PrimaryNode == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildRedirectUrlWithQuery null path primary object supplied");
                return null;
            }

            #endregion

            #region Variables

            string url = "";
            Node availableNode = null;

            #endregion

            #region Process

            #region Check-for-Owner-First

            foreach (Node currNode in topology.Nodes)
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
                        logging.Log(LoggingModule.Severity.Warn, "BuildRedirectUrlWithQuery owning NodeId " + currNode.NodeId + " is unavailable (failure count greater than zero), searching for peer");
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
                    foreach (Node currNode in topology.Nodes)
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
                                logging.Log(LoggingModule.Severity.Warn, "BuildRedirectUrlWithQuery neighbor NodeId " + currNode.NodeId + " is unavailable (failure count greater than zero), searching for peer");
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
                logging.Log(LoggingModule.Severity.Warn, "BuildRedirectUrlWithQuery no available neighbor found");
                return null;
            }

            #endregion

            #endregion
        }
        
        public static string BuildDiskPath(Obj currObj, UserMaster currUser, Settings settings, Events logging)
        {
            #region Check-for-Null-Values

            if (currObj == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath null object supplied");
                return null;
            }

            if (currUser == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath null user supplied");
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
                homeDirectory = String.Copy(settings.Storage.Directory);
                if (!homeDirectory.EndsWith(Common.GetPathSeparator(settings.Environment))) homeDirectory += Common.GetPathSeparator(settings.Environment);
                homeDirectory += currUser.Guid;
                homeDirectory += Common.GetPathSeparator(settings.Environment);
            }
            else
            {
                // user-specific home directory
                homeDirectory = String.Copy(currUser.HomeDirectory);
                if (!homeDirectory.EndsWith(Common.GetPathSeparator(settings.Environment))) homeDirectory += Common.GetPathSeparator(settings.Environment);
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
                            logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath unsafe characters detected: " + currContainer);
                            return null;
                        }

                        fullPath += currContainer + Common.GetPathSeparator(settings.Environment);
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

        public static bool UnsafeFsChars(Obj currObj)
        {
            if (currObj == null) return true;
            if (currObj.ContainerPath != null && currObj.ContainerPath.Count > 0)
            {
                if (Common.ContainsUnsafeCharacters(currObj.ContainerPath)) return true;
            }
            if (!String.IsNullOrEmpty(currObj.Key))
            {
                if (Common.ContainsUnsafeCharacters(currObj.Key)) return true;
            }
            return false;
        }

        #endregion

        #region Private-Static-Methods
        
        #endregion
    }
}
