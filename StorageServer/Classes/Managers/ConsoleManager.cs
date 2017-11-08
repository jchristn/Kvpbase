using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using RestWrapper;

namespace Kvpbase
{
    public class ConsoleManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private bool _Enabled { get; set; }
        private Settings _Settings { get; set; }
        private Topology _Topology { get; set; }
        private Node _Node { get; set; }
        private UserManager _UserMgr { get; set; }
        private UrlLockManager _UrlLockMgr { get; set; }
        private EncryptionModule _Encryption { get; set; }
        private Events _Logging { get; set; }
        private MaintenanceManager _MaintenanceMgr { get; set; }
        private Func<bool> _ExitDelegate;

        #endregion

        #region Constructors-and-Factories

        public ConsoleManager(
            Settings settings, 
            MaintenanceManager maintenance, 
            Topology topology, 
            Node node, 
            UserManager users, 
            UrlLockManager locks,
            EncryptionModule encryption, 
            Events logging,
            Func<bool> exitApplication)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (maintenance == null) throw new ArgumentNullException(nameof(maintenance));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (users == null) throw new ArgumentNullException(nameof(users));
            if (locks == null) throw new ArgumentNullException(nameof(locks));
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (encryption == null) throw new ArgumentNullException(nameof(encryption));
            if (exitApplication == null) throw new ArgumentNullException(nameof(exitApplication));

            _Enabled = true;
            _Settings = settings;
            _Topology = topology;
            _Node = node;
            _UserMgr = users;
            _UrlLockMgr = locks;
            _Logging = logging;
            _MaintenanceMgr = maintenance;
            _Encryption = encryption;
            _ExitDelegate = exitApplication;

            Task.Run(() => ConsoleWorker());
        }

        #endregion

        #region Public-Methods

        public void Stop()
        {
            _Enabled = false;
            return;
        }

        #endregion

        #region Private-Methods

        private void ConsoleWorker()
        {
            string userInput = "";
            while (_Enabled)
            {
                Console.Write("Command (? for help) > ");
                userInput = Console.ReadLine();

                if (userInput == null) continue;
                switch (userInput.ToLower().Trim())
                {
                    case "?":
                        Menu();
                        break;

                    case "c":
                    case "cls":
                    case "clear":
                        Console.Clear();
                        break;

                    case "q":
                    case "quit":
                        _Enabled = false;
                        _ExitDelegate();
                        break;

                    case "find_obj":
                        FindObject(_Topology);
                        break;

                    case "list_topology":
                        ListTopology();
                        break;

                    case "list_active_urls":
                        ListActiveUrls();
                        break;

                    case "maint_enable":
                        _MaintenanceMgr.Set();
                        Console.WriteLine("Maintenance mode enabled");
                        break;

                    case "maint_disable":
                        _MaintenanceMgr.Stop();
                        Console.WriteLine("Maintenance mode disabled");
                        break;

                    case "maint_status":
                        Console.WriteLine("Maintenance enabled: " + _MaintenanceMgr.IsEnabled());
                        break;

                    case "data_validation":
                        DataValidation();
                        break;

                    case "version":
                        Console.WriteLine(_Settings.ProductVersion);
                        break;

                    case "debug_on":
                        _Settings.Syslog.MinimumLevel = 0;
                        break;

                    case "debug_off":
                        _Settings.Syslog.MinimumLevel = 1;
                        break;

                    default:
                        Console.WriteLine("Unknown command.  '?' for help.");
                        break;
                }
            }
        }

        private void Menu()
        {
            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("  ?                         help / this menu");
            Console.WriteLine("  cls / c                   clear the console");
            Console.WriteLine("  quit / q                  exit the application");
            Console.WriteLine("  server                    list endpoint addresses for this node");
            Console.WriteLine("  find_obj                  locate an object by primary GUID and object name");
            Console.WriteLine("  list_topology             list nodes in the topology");
            Console.WriteLine("  list_active_urls          list URLs that are locked or being read");
            Console.WriteLine("  maint_enable              enable read broadcast and maintenance mode");
            Console.WriteLine("  maint_disable             disable read broadcast and maintenance mode");
            Console.WriteLine("  maint_status              display maintenance mode status");
            Console.WriteLine("  data_validation           validate data stored on this node");
            Console.WriteLine("  version                   show the product version");
            Console.WriteLine("");
            return;
        }

        private void FindObject(Topology topology)
        {
            Console.Write("Node name [ENTER for all]: ");
            string node = Console.ReadLine();

            Console.Write("User GUID: ");
            string userGuid = Console.ReadLine();

            Console.Write("Object key: ");
            string key = Console.ReadLine();

            List<string> containers = new List<string>();
            Console.WriteLine("Enter each container name, starting from the root.  Do not enter the user GUID.");
            Console.WriteLine("When finished, press ENTER.");
            while (true)
            {
                Console.Write("  Container: ");
                string container = Console.ReadLine();
                if (String.IsNullOrEmpty(container)) break;
                containers.Add(container);
            }

            if (String.IsNullOrEmpty(userGuid) || String.IsNullOrEmpty(key))
            {
                Console.WriteLine("Both user GUID and object name must be populated.");
                return;
            }

            Find req = new Find();
            req.UserGuid = userGuid;
            req.Key = key;
            req.ContainerPath = containers;

            List<string> found = new List<string>();
            if (topology != null && topology.Nodes != null)
            {
                foreach (Node curr in topology.Nodes)
                {
                    if ((String.IsNullOrEmpty(node)) ||
                        (String.Compare(curr.Name, node) == 0))
                    {
                        if (FindObjectOnPeer(curr, req))
                        {
                            found.Add("Found on: " + curr.Name + " " + curr.DnsHostname + ":" + curr.Port);
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("No nodes in topology");
            }

            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("");

            if (found != null)
            {
                if (found.Count > 0)
                {
                    Console.WriteLine("Search for key " + key + " for user GUID " + userGuid + " found on the following nodes:");
                    Console.WriteLine("");

                    foreach (string s in found)
                    {
                        Console.WriteLine("  " + s);
                    }
                }
                else
                {
                    Console.WriteLine("Not found (empty list)");
                }
            }
            else
            {
                Console.WriteLine("Not found (null)");
            }

            Console.WriteLine("");
            Console.WriteLine(Common.Line(79, "-"));

            return;
        }

        private bool FindObjectOnPeer(Node curr, Find req)
        {
            string url = "";
            if (Common.IsTrue(curr.Ssl)) url = "https://" + curr.DnsHostname + ":" + curr.Port + "/admin/find";
            else url = "http://" + curr.DnsHostname + ":" + curr.Port + "/admin/find";

            Dictionary<string, string> headers = Common.AddToDictionary(_Settings.Server.HeaderApiKey, _Settings.Server.AdminApiKey, null);
            req.QueryTopology = false;
            
            RestWrapper.RestResponse resp = RestRequest.SendRequestSafe(
                url, "application/json", "POST", null, null, false,
                Common.IsTrue(_Settings.Rest.AcceptInvalidCerts), headers,
                Encoding.UTF8.GetBytes(Common.SerializeJson(req)));

            if (resp == null) return false;
            if (resp.StatusCode != 200) return false;
            
            return true;
        }

        private void ListTopology()
        {
            if (_Topology == null || _Topology.Nodes == null || _Topology.Nodes.Count < 1)
            {
                Console.WriteLine("Topology contains no nodes");
            }
            else
            {
                Console.WriteLine("Nodes in topology:");
                foreach (Node curr in _Topology.Nodes)
                {
                    Console.WriteLine(curr.ToString());
                }

                Console.WriteLine("");
                if (_Topology.Replicas == null || _Topology.Replicas.Count < 1)
                {
                    Console.WriteLine("No replicas defined in topology");
                }
                else
                {
                    Console.WriteLine("Replica nodes:");
                    foreach (Node curr in _Topology.Replicas)
                    {
                        Console.WriteLine(curr.ToString());
                    }
                }
            }

            Console.WriteLine("");
        }

        private void ListActiveUrls()
        {
            Dictionary<string, Tuple<int?, string, string, DateTime>> lockedUrls = _UrlLockMgr.GetLockedUrls();
            List<string> readUrls = _UrlLockMgr.GetReadUrls();

            if (lockedUrls == null || lockedUrls.Count < 1)
            {
                Console.WriteLine("No locked objects");
            }
            else
            {
                Console.WriteLine("Locked objects: " + lockedUrls.Count);
                foreach (KeyValuePair<string, Tuple<int?, string, string, DateTime>> curr in lockedUrls)
                {
                    // UserMasterId, SourceIp, verb, established
                    Console.WriteLine("  " + curr.Key + " user " + curr.Value.Item1 + " " + curr.Value.Item2 + " " + curr.Value.Item3);
                }
                Console.WriteLine("");
            }
            if (lockedUrls == null || lockedUrls.Count < 1)
            {
                Console.WriteLine("No locked URLs");
            }

            if (readUrls == null || readUrls.Count < 1)
            {
                Console.WriteLine("No objects being read");
            }
            else
            {
                Console.WriteLine("Read objects: " + readUrls.Count);
                foreach (string curr in readUrls)
                {
                    Console.WriteLine("  " + curr);
                }
                Console.WriteLine("");
            }
        }

        private void DataValidation()
        {
            DateTime startTime = DateTime.Now;
            int usersProcessed = 0;
            int usersMoved = 0;
            int subdirsMoved = 0;
            int filesMoved = 0;

            try
            {
                #region Check-for-Maintenance-Mode

                if (!_MaintenanceMgr.IsEnabled())
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "DataValidation cannot begin, maintenance mode not enabled");
                    return;
                }

                #endregion

                #region Enumerate

                _Logging.Log(LoggingModule.Severity.Debug, "DataValidation starting at " + DateTime.Now);

                #endregion

                #region Variables

                string currUserGuid = "";
                UserMaster currUser = new UserMaster();
                Node currOwner = new Node();
                List<string> userDirs = new List<string>();
                List<string> userSubdirs = new List<string>();
                List<string> userFiles = new List<string>();
                bool errorDetected = false;

                Dictionary<string, string> headers = new Dictionary<string, string>();
                string subdirUrl = "";
                RestResponse createSubdirResp = new RestResponse();
                bool deleteDirSuccess = false;

                Obj currObj = new Obj();
                string fileUrl = "";
                RestResponse createFileResp = new RestResponse();
                bool deleteFileSuccess = false;

                #endregion

                #region Gather-Local-User-GUIDs

                userDirs = Common.GetSubdirectoryList(_Settings.Storage.Directory, false);
                if (userDirs == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "DataValidation no subdirectories found (null) in " + _Settings.Storage.Directory);
                    return;
                }

                if (userDirs.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "DataValidation no subdirectories found (empty) in " + _Settings.Storage.Directory);
                    return;
                }

                #endregion

                #region Process

                foreach (string currUserDir in userDirs)
                {
                    #region Reset-Variables

                    currUserGuid = "";
                    currUser = null;
                    currOwner = new Node();
                    userSubdirs = new List<string>();
                    userFiles = new List<string>();
                    headers = new Dictionary<string, string>();
                    errorDetected = false;
                    deleteDirSuccess = false;

                    usersProcessed++;

                    #endregion

                    #region Derive-User-GUID

                    currUserGuid = currUserDir.Replace(_Settings.Storage.Directory, "");
                    if (String.IsNullOrEmpty(currUserGuid))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "DataValidation skipping subdirectory " + currUserDir + " (unable to derive user GUID");
                        continue;
                    }

                    _Logging.Log(LoggingModule.Severity.Info, "DataValidation processing " + currUserDir + " (user GUID " + currUserGuid + ")");

                    #endregion

                    #region Retrieve-User-Record

                    currUser = _UserMgr.GetUserByGuid(currUserGuid);
                    if (currUser == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "DataValidation found user GUID " + currUserGuid + " on disk but no matching user record");
                        continue;
                    }

                    #endregion

                    #region Determine-Owning-Node

                    if (currUser.NodeId != null)
                    {
                        if (currUser.NodeId == _Node.NodeId)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "DataValidation user GUID " + currUserGuid + " remains on this node (pinned)");
                            continue;
                        }
                    }

                    currOwner = Node.DetermineOwner(currUserGuid, _UserMgr, _Topology, _Node, _Logging);
                    if (currOwner == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to calculate primary for user GUID " + currUserGuid);
                        continue;
                    }

                    if (currOwner.NodeId == _Node.NodeId)
                    {
                        // we are the primary
                        _Logging.Log(LoggingModule.Severity.Debug, "DataValidation user GUID " + currUserGuid + " remains on this node (calculated)");
                        continue;
                    }
                    else
                    {
                        bool isNeighbor = false;
                        foreach (Node currNode in _Topology.Nodes)
                        {
                            if (currNode.NodeId == currOwner.NodeId)
                            {
                                // check the neighbor list to see if we are a neighbor
                                foreach (int currNodeId in currNode.Neighbors)
                                {
                                    if (currNodeId == _Node.NodeId)
                                    {
                                        // we are a neighbor
                                        isNeighbor = true;
                                        break;
                                    }
                                }

                                if (isNeighbor) break;
                            }
                        }

                        if (isNeighbor)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "DataValidation user GUID " + currUserGuid + " remains on this node (local copy is replica)");
                            continue;
                        }
                    }

                    #endregion

                    #region Retrieve-Subdirectory-List

                    userSubdirs = Common.GetSubdirectoryList(_Settings.Storage.Directory + currUserGuid, true);
                    if (userSubdirs == null) userSubdirs = new List<string>();
                    userSubdirs.Add(_Settings.Storage.Directory + currUserGuid);

                    #endregion

                    #region Create-User-Headers

                    headers = Common.AddToDictionary("x-email", currUser.Email, headers);
                    headers = Common.AddToDictionary("x-password", currUser.Password, headers);

                    #endregion

                    #region Process-Each-Subdirectory

                    foreach (string currUserSubdir in userSubdirs)
                    {
                        #region Reset-Variables

                        subdirUrl = "";
                        createSubdirResp = new RestResponse();

                        #endregion

                        #region Build-Subdirectory-URL

                        subdirUrl = Obj.BuildUrlFromFilePath(currUserSubdir, currOwner, null, _UserMgr, _Settings, _Logging);
                        _Logging.Log(LoggingModule.Severity.Debug, "DataValidation processing subdirectory path " + currUserSubdir);
                        _Logging.Log(LoggingModule.Severity.Debug, "DataValidation processing subdirectory URL " + subdirUrl);

                        if (String.IsNullOrEmpty(subdirUrl))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to build URL for subdirectory " + currUserSubdir);
                            continue;
                        }

                        #endregion

                        #region Create-New-Subdirectory

                        createSubdirResp = RestRequest.SendRequestSafe(
                            subdirUrl + "&container=true", null, "PUT", null, null, false,
                            Common.IsTrue(_Settings.Rest.AcceptInvalidCerts),
                            headers,
                            null);

                        if (createSubdirResp == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "DataValidation null REST response for user GUID " + currUserGuid + " while creating subdirectory " + subdirUrl);
                            errorDetected = true;
                            continue;
                        }

                        if (createSubdirResp.StatusCode != 200 && createSubdirResp.StatusCode != 201)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "DataValidation non-200/201 REST response for user GUID " + currUserGuid + " while creating subdirectory " + subdirUrl);
                            errorDetected = true;
                            continue;
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "DataValidation successfully created user GUID " + currUserGuid + " subdirectory " + subdirUrl);

                        #endregion

                        #region Get-File-List

                        userFiles = Common.GetFileList(_Settings.Environment, currUserSubdir, true);
                        if (userFiles == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "DataValidation null file list for user GUID " + currUserGuid + " in directory " + currUserSubdir);
                            continue;
                        }

                        if (userFiles.Count < 1)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "DataValidation empty file list for user GUID " + currUserGuid + " in directory " + currUserSubdir);
                            continue;
                        }

                        #endregion

                        #region Process-Each-File

                        foreach (string currUserFile in userFiles)
                        {
                            #region Reset-Variables

                            currObj = new Obj();
                            fileUrl = "";
                            createFileResp = new RestResponse();
                            deleteFileSuccess = false;

                            #endregion

                            #region Build-Object

                            currObj = Obj.BuildObjFromDisk(currUserFile, _UserMgr, _Settings, _Topology, _Node, _Logging);
                            if (currObj == null)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to retrieve obj for " + currUserFile);
                                continue;
                            }

                            #endregion

                            #region Build-File-URL

                            fileUrl = Obj.BuildUrlFromFilePath(currUserFile, currOwner, currObj, _UserMgr, _Settings, _Logging);
                            if (String.IsNullOrEmpty(fileUrl))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to build file URL for user GUID " + currUserGuid + " file " + currUserFile);
                                continue;
                            }

                            #endregion

                            #region Decrypt-to-Compressed-Value

                            if (Common.IsTrue(currObj.IsEncrypted))
                            {
                                byte[] cleartext;

                                if (String.IsNullOrEmpty(currObj.EncryptionKsn))
                                {
                                    // server encryption
                                    if (!_Encryption.ServerDecrypt(currObj.Value, currObj.EncryptionKsn, out cleartext))
                                    {
                                        _Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to decrypt file for user GUID " + currUserGuid + " file " + currUserFile);
                                        continue;
                                    }

                                    if (cleartext == null)
                                    {
                                        _Logging.Log(LoggingModule.Severity.Warn, "DataValidation null value after server decryption for user GUID " + currUserGuid + " file " + currUserFile);
                                        continue;
                                    }
                                }
                                else
                                {
                                    cleartext = _Encryption.LocalDecrypt(currObj.Value);
                                    if (cleartext == null)
                                    {
                                        _Logging.Log(LoggingModule.Severity.Warn, "DataValidation null value after local decryption for user GUID " + currUserGuid + " file " + currUserFile);
                                        continue;
                                    }
                                }

                                currObj.Value = cleartext;
                            }

                            #endregion

                            #region Decompress-to-Base64-Encoded-Value

                            if (Common.IsTrue(currObj.IsCompressed))
                            {
                                currObj.Value = Common.GzipDecompress(currObj.Value);
                            }

                            #endregion

                            #region Write-File

                            createFileResp = RestRequest.SendRequestSafe(
                                subdirUrl + "&encoded=true", currObj.ContentType, "PUT", null, null, false,
                                Common.IsTrue(_Settings.Rest.AcceptInvalidCerts),
                                headers,
                                currObj.Value);

                            if (createFileResp == null)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "DataValidation null REST response for user GUID " + currUserGuid + " while creating file " + fileUrl);
                                errorDetected = true;
                                continue;
                            }

                            if (createFileResp.StatusCode != 200 && createFileResp.StatusCode != 201)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "DataValidation non-200/201 REST response for user GUID " + currUserGuid + " while creating file " + fileUrl);
                                errorDetected = true;
                                continue;
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "DataValidation successfully created user GUID " + currUserGuid + " file " + fileUrl);

                            #endregion

                            #region Delete-Local-Copy

                            deleteFileSuccess = Common.DeleteFile(currUserFile);
                            if (!deleteFileSuccess)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to delete user GUID " + currUserGuid + " file " + currUserFile);
                                continue;
                            }

                            #endregion

                            filesMoved++;
                        }

                        #endregion

                        subdirsMoved++;
                    }

                    #endregion

                    #region Delete-User-Directory

                    //
                    // IMPORTANT: since the order of subdirectories returned cannot be guaranteed
                    // and we can't be sure that deepest paths will be processed first, subdirectories
                    // should not be deleted until finished.  Instead, the user directory itself
                    // should be deleted recursively and only when no failure has been detected
                    //
                    // the 'error_detected' variable should be set to true to indicate when an 
                    // attempt to write data to the target machine has failed (and only used for this
                    // specific case).  The local copy should only be deleted when this variable is
                    // set to false.  The variable should be reset to false on each user_guid
                    //
                    if (!errorDetected)
                    {
                        deleteDirSuccess = Common.DeleteDirectory(currUserDir, true);
                        if (!deleteDirSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to delete user directory " + currUserDir);
                            continue;
                        }
                    }

                    #endregion

                    usersMoved++;
                }

                #endregion
            }
            catch (Exception e)
            {
                _Logging.Exception("DataValidation", "Outer exception", e);
                Common.ExitApplication("DataValidation", "Outer exception", -1);
                return;
            }
            finally
            {
                _Logging.Log(LoggingModule.Severity.Debug, Common.Line(79, "-"));
                _Logging.Log(LoggingModule.Severity.Debug, "DataValidation finished after " + Common.TotalMsFrom(startTime) + "ms");
                _Logging.Log(LoggingModule.Severity.Debug, "  " + usersProcessed + " user GUIDs processed");
                _Logging.Log(LoggingModule.Severity.Debug, "  " + usersMoved + " user GUIDs moved");
                _Logging.Log(LoggingModule.Severity.Debug, "  " + subdirsMoved + " subdirectorectories moved");
                _Logging.Log(LoggingModule.Severity.Debug, "  " + filesMoved + " files moved");
                _Logging.Log(LoggingModule.Severity.Debug, Common.Line(79, "-"));
            }
        }

        #endregion 
    }
}
