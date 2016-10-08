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

        private bool Enabled { get; set; }
        private Settings CurrentSettings { get; set; }
        private Topology CurrentTopology { get; set; }
        private Node CurrentNode { get; set; }
        private UserManager Users { get; set; }
        private UrlLockManager LockManager { get; set; }
        private EncryptionModule Encryption { get; set; }
        private Events Logging { get; set; }
        private MaintenanceManager Maintenance { get; set; }
        private Func<bool> ExitApplicationDelegate;

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

            Enabled = true;
            CurrentSettings = settings;
            CurrentTopology = topology;
            CurrentNode = node;
            Users = users;
            LockManager = locks;
            Logging = logging;
            Maintenance = maintenance;
            Encryption = encryption;
            ExitApplicationDelegate = exitApplication;

            Task.Run(() => ConsoleWorker());
        }

        #endregion

        #region Public-Methods

        public void Stop()
        {
            Enabled = false;
            return;
        }

        #endregion

        #region Private-Methods

        private void ConsoleWorker()
        {
            string userInput = "";
            while (Enabled)
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
                        Enabled = false;
                        ExitApplicationDelegate();
                        break;

                    case "find_obj":
                        FindObject(CurrentTopology);
                        break;

                    case "list_topology":
                        ListTopology();
                        break;

                    case "list_active_urls":
                        ListActiveUrls();
                        break;

                    case "maint_enable":
                        Maintenance.Set();
                        Console.WriteLine("Maintenance mode enabled");
                        break;

                    case "maint_disable":
                        Maintenance.Stop();
                        Console.WriteLine("Maintenance mode disabled");
                        break;

                    case "maint_status":
                        Console.WriteLine("Maintenance enabled: " + Maintenance.IsEnabled());
                        break;

                    case "data_validation":
                        DataValidation();
                        break;

                    case "version":
                        Console.WriteLine(CurrentSettings.ProductVersion);
                        break;

                    case "debug_on":
                        CurrentSettings.Syslog.MinimumLevel = 0;
                        break;

                    case "debug_off":
                        CurrentSettings.Syslog.MinimumLevel = 1;
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

            Dictionary<string, string> headers = Common.AddToDictionary(CurrentSettings.Server.HeaderApiKey, CurrentSettings.Server.AdminApiKey, null);
            req.QueryTopology = false;
            
            RestWrapper.RestResponse resp = RestRequest.SendRequestSafe(
                url, "application/json", "POST", null, null, false,
                Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts), headers,
                Encoding.UTF8.GetBytes(Common.SerializeJson(req)));

            if (resp == null) return false;
            if (resp.StatusCode != 200) return false;
            
            return true;
        }

        private void ListTopology()
        {
            if (CurrentTopology == null || CurrentTopology.Nodes == null || CurrentTopology.Nodes.Count < 1)
            {
                Console.WriteLine("Topology contains no nodes");
            }
            else
            {
                Console.WriteLine("Nodes in topology:");
                foreach (Node curr in CurrentTopology.Nodes)
                {
                    Console.WriteLine(curr.ToString());
                }

                Console.WriteLine("");
                if (CurrentTopology.Replicas == null || CurrentTopology.Replicas.Count < 1)
                {
                    Console.WriteLine("No replicas defined in topology");
                }
                else
                {
                    Console.WriteLine("Replica nodes:");
                    foreach (Node curr in CurrentTopology.Replicas)
                    {
                        Console.WriteLine(curr.ToString());
                    }
                }
            }

            Console.WriteLine("");
        }

        private void ListActiveUrls()
        {
            Dictionary<string, Tuple<int?, string, string, DateTime>> lockedUrls = LockManager.GetLockedUrls();
            List<string> readUrls = LockManager.GetReadUrls();

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

                if (!Maintenance.IsEnabled())
                {
                    Logging.Log(LoggingModule.Severity.Warn, "DataValidation cannot begin, maintenance mode not enabled");
                    return;
                }

                #endregion

                #region Enumerate

                Logging.Log(LoggingModule.Severity.Debug, "DataValidation starting at " + DateTime.Now);

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

                userDirs = Common.GetSubdirectoryList(CurrentSettings.Storage.Directory, false);
                if (userDirs == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "DataValidation no subdirectories found (null) in " + CurrentSettings.Storage.Directory);
                    return;
                }

                if (userDirs.Count < 1)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "DataValidation no subdirectories found (empty) in " + CurrentSettings.Storage.Directory);
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

                    currUserGuid = currUserDir.Replace(CurrentSettings.Storage.Directory, "");
                    if (String.IsNullOrEmpty(currUserGuid))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "DataValidation skipping subdirectory " + currUserDir + " (unable to derive user GUID");
                        continue;
                    }

                    Logging.Log(LoggingModule.Severity.Info, "DataValidation processing " + currUserDir + " (user GUID " + currUserGuid + ")");

                    #endregion

                    #region Retrieve-User-Record

                    currUser = Users.GetUserByGuid(currUserGuid);
                    if (currUser == null)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "DataValidation found user GUID " + currUserGuid + " on disk but no matching user_master record");
                        continue;
                    }

                    #endregion

                    #region Determine-Owning-Node

                    if (currUser.NodeId != null)
                    {
                        if (currUser.NodeId == CurrentNode.NodeId)
                        {
                            Logging.Log(LoggingModule.Severity.Debug, "DataValidation user GUID " + currUserGuid + " remains on this node (pinned)");
                            continue;
                        }
                    }

                    currOwner = Node.DetermineOwner(currUserGuid, Users, CurrentTopology, CurrentNode, Logging);
                    if (currOwner == null)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to calculate primary for user GUID " + currUserGuid);
                        continue;
                    }

                    if (currOwner.NodeId == CurrentNode.NodeId)
                    {
                        // we are the primary
                        Logging.Log(LoggingModule.Severity.Debug, "DataValidation user GUID " + currUserGuid + " remains on this node (calculated)");
                        continue;
                    }
                    else
                    {
                        bool isNeighbor = false;
                        foreach (Node currNode in CurrentTopology.Nodes)
                        {
                            if (currNode.NodeId == currOwner.NodeId)
                            {
                                // check the neighbor list to see if we are a neighbor
                                foreach (int currNodeId in currNode.Neighbors)
                                {
                                    if (currNodeId == CurrentNode.NodeId)
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
                            Logging.Log(LoggingModule.Severity.Debug, "DataValidation user GUID " + currUserGuid + " remains on this node (local copy is replica)");
                            continue;
                        }
                    }

                    #endregion

                    #region Retrieve-Subdirectory-List

                    userSubdirs = Common.GetSubdirectoryList(CurrentSettings.Storage.Directory + currUserGuid, true);
                    if (userSubdirs == null) userSubdirs = new List<string>();
                    userSubdirs.Add(CurrentSettings.Storage.Directory + currUserGuid);

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

                        subdirUrl = Obj.BuildUrlFromFilePath(currUserSubdir, currOwner, null, Users, CurrentSettings, Logging);
                        Logging.Log(LoggingModule.Severity.Debug, "DataValidation processing subdirectory path " + currUserSubdir);
                        Logging.Log(LoggingModule.Severity.Debug, "DataValidation processing subdirectory URL " + subdirUrl);

                        if (String.IsNullOrEmpty(subdirUrl))
                        {
                            Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to build URL for subdirectory " + currUserSubdir);
                            continue;
                        }

                        #endregion

                        #region Create-New-Subdirectory

                        createSubdirResp = RestRequest.SendRequestSafe(
                            subdirUrl + "&container=true", null, "PUT", null, null, false,
                            Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                            headers,
                            null);

                        if (createSubdirResp == null)
                        {
                            Logging.Log(LoggingModule.Severity.Warn, "DataValidation null REST response for user GUID " + currUserGuid + " while creating subdirectory " + subdirUrl);
                            errorDetected = true;
                            continue;
                        }

                        if (createSubdirResp.StatusCode != 200 && createSubdirResp.StatusCode != 201)
                        {
                            Logging.Log(LoggingModule.Severity.Warn, "DataValidation non-200/201 REST response for user GUID " + currUserGuid + " while creating subdirectory " + subdirUrl);
                            errorDetected = true;
                            continue;
                        }

                        Logging.Log(LoggingModule.Severity.Debug, "DataValidation successfully created user GUID " + currUserGuid + " subdirectory " + subdirUrl);

                        #endregion

                        #region Get-File-List

                        userFiles = Common.GetFileList(CurrentSettings.Environment, currUserSubdir, true);
                        if (userFiles == null)
                        {
                            Logging.Log(LoggingModule.Severity.Debug, "DataValidation null file list for user GUID " + currUserGuid + " in directory " + currUserSubdir);
                            continue;
                        }

                        if (userFiles.Count < 1)
                        {
                            Logging.Log(LoggingModule.Severity.Debug, "DataValidation empty file list for user GUID " + currUserGuid + " in directory " + currUserSubdir);
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

                            currObj = Obj.BuildObjFromDisk(currUserFile, Users, CurrentSettings, CurrentTopology, CurrentNode, Logging);
                            if (currObj == null)
                            {
                                Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to retrieve obj for " + currUserFile);
                                continue;
                            }

                            #endregion

                            #region Build-File-URL

                            fileUrl = Obj.BuildUrlFromFilePath(currUserFile, currOwner, currObj, Users, CurrentSettings, Logging);
                            if (String.IsNullOrEmpty(fileUrl))
                            {
                                Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to build file URL for user GUID " + currUserGuid + " file " + currUserFile);
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
                                    if (!Encryption.ServerDecrypt(currObj.Value, currObj.EncryptionKsn, out cleartext))
                                    {
                                        Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to decrypt file for user GUID " + currUserGuid + " file " + currUserFile);
                                        continue;
                                    }

                                    if (cleartext == null)
                                    {
                                        Logging.Log(LoggingModule.Severity.Warn, "DataValidation null value after server decryption for user GUID " + currUserGuid + " file " + currUserFile);
                                        continue;
                                    }
                                }
                                else
                                {
                                    cleartext = Encryption.LocalDecrypt(currObj.Value);
                                    if (cleartext == null)
                                    {
                                        Logging.Log(LoggingModule.Severity.Warn, "DataValidation null value after local decryption for user GUID " + currUserGuid + " file " + currUserFile);
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
                                Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                                headers,
                                currObj.Value);

                            if (createFileResp == null)
                            {
                                Logging.Log(LoggingModule.Severity.Warn, "DataValidation null REST response for user GUID " + currUserGuid + " while creating file " + fileUrl);
                                errorDetected = true;
                                continue;
                            }

                            if (createFileResp.StatusCode != 200 && createFileResp.StatusCode != 201)
                            {
                                Logging.Log(LoggingModule.Severity.Warn, "DataValidation non-200/201 REST response for user GUID " + currUserGuid + " while creating file " + fileUrl);
                                errorDetected = true;
                                continue;
                            }

                            Logging.Log(LoggingModule.Severity.Debug, "DataValidation successfully created user GUID " + currUserGuid + " file " + fileUrl);

                            #endregion

                            #region Delete-Local-Copy

                            deleteFileSuccess = Common.DeleteFile(currUserFile);
                            if (!deleteFileSuccess)
                            {
                                Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to delete user GUID " + currUserGuid + " file " + currUserFile);
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
                            Logging.Log(LoggingModule.Severity.Warn, "DataValidation unable to delete user directory " + currUserDir);
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
                Logging.Exception("DataValidation", "Outer exception", e);
                Common.ExitApplication("DataValidation", "Outer exception", -1);
                return;
            }
            finally
            {
                Logging.Log(LoggingModule.Severity.Debug, Common.Line(79, "-"));
                Logging.Log(LoggingModule.Severity.Debug, "DataValidation finished after " + Common.TotalMsFrom(startTime) + "ms");
                Logging.Log(LoggingModule.Severity.Debug, "  " + usersProcessed + " user GUIDs processed");
                Logging.Log(LoggingModule.Severity.Debug, "  " + usersMoved + " user GUIDs moved");
                Logging.Log(LoggingModule.Severity.Debug, "  " + subdirsMoved + " subdirectorectories moved");
                Logging.Log(LoggingModule.Severity.Debug, "  " + filesMoved + " files moved");
                Logging.Log(LoggingModule.Severity.Debug, Common.Line(79, "-"));
            }
        }

        #endregion

        #region Public-Static-Methods

        #endregion

        #region Private-Static-Methods

        #endregion
    }
}
