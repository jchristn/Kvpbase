using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace Kvpbase.Classes.Managers
{
    public class TaskManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private TopologyManager _Topology;

        private string _LocalTaskDirectory;
        private string _TimestampFormat = "yyyy-MM-ddTHH-mm-ss-ffffffZ";

        #endregion

        #region Constructors-and-Factories

        public TaskManager(Settings settings, LoggingModule logging, TopologyManager topology)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Tasks == null) throw new ArgumentException("Tasks section is not populated in configuration.");
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (topology == null) throw new ArgumentNullException(nameof(topology));

            _Settings = settings;
            _Logging = logging;
            _Topology = topology;

            if (String.IsNullOrEmpty(settings.Tasks.Directory))
            {
                _Logging.Log(LoggingModule.Severity.Info, "TaskManager no directory, specified, using ./Tasks/");
                settings.Tasks.Directory = "./Tasks/";
            }

            if (!Directory.Exists(settings.Tasks.Directory)) Directory.CreateDirectory(settings.Tasks.Directory);

            _LocalTaskDirectory = settings.Tasks.Directory + "local/";
            if (!Directory.Exists(_LocalTaskDirectory)) Directory.CreateDirectory(_LocalTaskDirectory);

            if (settings.Tasks.RefreshSec < 1)
            {
                _Logging.Log(LoggingModule.Severity.Info, "TaskManager invalid value for refresh interval, using 10 seconds");
                settings.Tasks.RefreshSec = 10;
            }

            Task.Run(() => TaskWorker());
        }
         
        #endregion

        #region Public-Methods
        
        public bool Add(TaskObject taskObj)
        {
            if (taskObj == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "Add no task supplied");
                return false;
            }

            if (taskObj.RecipientNodeId < 0)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "Add invalid recipient node ID supplied: " + taskObj.RecipientNodeId);
                return false;
            }

            string directory = "";
            string filename = "";

            if (taskObj.RecipientNodeId == 0)
            { 
                directory = _LocalTaskDirectory;
            }
            else
            { 
                directory = _Settings.Tasks.Directory + taskObj.RecipientNodeId; 
            }

            filename = TaskFilenameAndPath(taskObj);

            if (!Directory.Exists(directory)) Directory.CreateDirectory(directory);
             
            byte[] bytes = Encoding.UTF8.GetBytes(Common.SerializeJson(taskObj, false));

            _Logging.Log(LoggingModule.Severity.Info, "Add adding task to file: " + filename);
            return Common.WriteFile(filename, bytes);
        }

        public int Count()
        {
            int ret = 0; 

            string directory = _LocalTaskDirectory;
            if (!Directory.Exists(directory)) return ret;
            ret =
                (from file in Directory.EnumerateFiles(@directory, "*", SearchOption.AllDirectories)
                 select file).Count();

            return ret;
        }

        public int Count(string recipient)
        {
            int ret = 0;
            if (String.IsNullOrEmpty(recipient)) return ret;

            string directory = _LocalTaskDirectory + recipient;
            if (!Directory.Exists(directory)) return ret;
            ret = 
                (from file in Directory.EnumerateFiles(@directory, "*", SearchOption.TopDirectoryOnly)
                 select file).Count();

            return ret;
        }

        #endregion

        #region Private-Methods
        
        private string TaskFilenameAndPath(TaskObject taskObj)
        { 
            if (taskObj.RecipientNodeId == 0)
            {
                return _LocalTaskDirectory + TaskFilename(taskObj);
            }
            else
            {
                return _Settings.Tasks.Directory + taskObj.RecipientNodeId + "/" + TaskFilename(taskObj);
            } 
        }

        private string TaskFilename(TaskObject task)
        {
            return Convert.ToDateTime(task.Created).ToString(_TimestampFormat) + ".json";
        }

        private void TaskWorker()
        {
            bool firstRun = true;
             
            while (true)
            {
                try
                {
                    #region Sleep

                    if (!firstRun)
                    {
                        Task.Delay(_Settings.Tasks.RefreshSec * 1000).Wait();
                    }
                    else
                    {
                        firstRun = false;
                    }

                    #endregion

                    #region Get-Subdirectories

                    List<string> directories = Directory.GetDirectories(_Settings.Tasks.Directory).ToList();
                    if (directories == null || directories.Count < 1) continue;

                    #endregion

                    #region Process-Each-Subdirectory

                    foreach (string directory in directories)
                    {
                        // _Logging.Log(LoggingModule.Severity.Debug, "TaskWorker processing directory " + directory);

                        #region Get-Files

                        List<string> files = Directory.GetFiles(directory).ToList();
                        if (files == null || files.Count < 1) continue;

                        #endregion

                        #region Process-Each-File

                        foreach (string currFile in files)
                        {
                            // _Logging.Log(LoggingModule.Severity.Debug, "TaskWorker processing file " + currFile);

                            #region Deserialize

                            TaskObject currTask = null;
                            try
                            {
                                currTask = Common.DeserializeJson<TaskObject>(Common.ReadBinaryFile(currFile));
                            }
                            catch (Exception e)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "TaskWorker unable to deserialize task file " + currFile + ": " + e.ToString());
                            }

                            if (currTask == null) continue;

                            #endregion

                            #region Check-Expiration

                            if (currTask.Expiration != null)
                            {
                                if (!Common.IsLaterThanNow(Convert.ToDateTime(currTask.Expiration)))
                                {
                                    _Logging.Log(LoggingModule.Severity.Info, "TaskWorker expired task found in file " + currFile + ", deleting");
                                    Common.DeleteFile(currFile);
                                    continue;
                                }
                            }

                            #endregion

                            #region Process-Task

                            bool success = false;
                            switch (currTask.Type)
                            {
                                case TaskType.Message:
                                    if (currTask.RecipientNodeId == 0)
                                    {
                                        #region Local-Message-Task

                                        #endregion
                                    }
                                    else
                                    {
                                        #region Remote-Message-Task

                                        Node currNode = _Topology.GetNodeById(currTask.RecipientNodeId);
                                        if (currNode == null)
                                        {
                                            _Logging.Log(LoggingModule.Severity.Warn, "TaskWorker unable to retrieve node ID " +
                                                currTask.RecipientNodeId + " " +
                                                "for task file " + currFile);
                                            continue;
                                        }

                                        Message currMessage = null;
                                        try
                                        {
                                            currMessage = ((JObject)currTask.Data).ToObject<Message>();
                                        }
                                        catch (Exception e)
                                        {
                                            _Logging.Log(LoggingModule.Severity.Warn, "TaskWorker unable to extract message object from task file " + currFile + ": " + e.ToString());
                                        }

                                        if (currMessage == null) continue;

                                        success = _Topology.SendAsyncMessage(currMessage);
                                        if (!success)
                                        {
                                            _Logging.Log(LoggingModule.Severity.Warn, "TaskWorker unable to send message to node " + currTask.RecipientNodeId);
                                        }

                                        #endregion
                                    }
                                    break;

                                default:
                                    _Logging.Log(LoggingModule.Severity.Warn, "TaskWorker unknown task type " + currTask.Type.ToString() + " in task file " + currFile);
                                    break;
                            }

                            if (success)
                            {
                                _Logging.Log(LoggingModule.Severity.Debug, "TaskWorker completed task file " + currFile);
                                Common.DeleteFile(currFile);
                            }

                            #endregion
                        }

                        #endregion
                    }

                    #endregion
                }
                catch (Exception e)
                {
                    _Logging.LogException("TaskManager", "TaskWorker", e);
                }
            } 
        }

        #endregion
    }
}
