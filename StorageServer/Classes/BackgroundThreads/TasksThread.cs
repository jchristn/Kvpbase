using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;

namespace Kvpbase
{
    public class TasksThread
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private Events _Logging;

        #endregion

        #region Constructors-and-Factories

        public TasksThread(Settings settings, Events logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            _Settings = settings;
            _Logging = logging;

            if (String.IsNullOrEmpty(_Settings.Tasks.Directory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TasksThread unable to open background task directory from configuration file");
                Common.ExitApplication("TasksThread", "Undefined tasks directory", -1);
                return;
            }

            if (!Common.VerifyDirectoryAccess(_Settings.Environment, _Settings.Tasks.Directory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TasksThread unable to access background task directory " + _Settings.Tasks.Directory);
                Common.ExitApplication("TasksThread", "Unable to access tasks directory", -1);
                return;
            }

            if (_Settings.Tasks.RefreshSec <= 0)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TasksThread setting background task retry timer to 10 sec, config value too low: " + _Settings.Tasks.RefreshSec + " sec");
                _Settings.Tasks.RefreshSec = 10;
            }

            Task.Run(() => TasksWorker());
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        private void TasksWorker()
        { 
            #region Process
             
            while (true)
            {
                #region Wait

                Task.Delay(_Settings.Tasks.RefreshSec * 1000).Wait(); 

                #endregion
                        
                #region Get-File-List

                List<string> tasks = new List<string>();
                tasks = Common.GetFileList(_Settings.Environment, _Settings.Tasks.Directory, false);

                if (tasks == null || tasks.Count < 1)
                {
                    continue;
                }

                #endregion

                #region Process-Each-File

                foreach (string currFile in tasks)
                {
                    #region Enumerate

                    _Logging.Log(LoggingModule.Severity.Debug, "TasksWorker processing task " + currFile);

                    #endregion

                    #region Read-File

                    string contents = Common.ReadTextFile(_Settings.Tasks.Directory + currFile);
                    if (String.IsNullOrEmpty(contents))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "TasksWorker empty file detected at " + currFile + ", deleting");
                        if (!Common.DeleteFile(_Settings.Tasks.Directory + currFile))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "TasksWorker unable to delete file " + currFile);
                            continue;
                        }
                    }

                    #endregion

                    #region Deserialize

                    TaskObject currTask = null;
                    try
                    {
                        currTask = Common.DeserializeJson<TaskObject>(contents);
                        if (currTask == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "TasksWorker unable to deserialize file " + currFile + ", skipping");
                            continue;
                        }
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "TasksWorker unable unable to deserialize file " + currFile + ", skipping");
                        continue;
                    }

                    #endregion

                    #region Process

                    _Logging.Log(LoggingModule.Severity.Debug, "TasksWorker starting background job to process task " + currFile);
                    Task.Run(() => TaskRunner(_Settings.Tasks.Directory + currFile, currTask));

                    #endregion
                }

                #endregion
            }

            #endregion
        }

        private void TaskRunner(string file, TaskObject currTask)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(file))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TaskRunner null value for file detected");
                return;
            }

            if (currTask == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TaskRunner null value for current task detected");
                return;
            }

            #endregion

            #region Check-for-Completion

            if (currTask.Completion != null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TaskRunner task already completed in file " + file + ", attempting to delete");
                Common.DeleteFile(file);
                return;
            }

            #endregion

            #region Process

            bool success = false;
            switch (currTask.TaskType)
            {
                case "loopback":
                    _Logging.Log(LoggingModule.Severity.Info, Common.Line(79, "-"));
                    _Logging.Log(LoggingModule.Severity.Info, "");
                    _Logging.Log(LoggingModule.Severity.Info, "Loopback task detected in file " + file);
                    _Logging.Log(LoggingModule.Severity.Info, "");
                    _Logging.Log(LoggingModule.Severity.Info, Common.Line(79, "-"));
                    success = true;
                    break;

                default:
                    _Logging.Log(LoggingModule.Severity.Warn, "TaskRunner unknown task type " + currTask.TaskType + " in file " + file);
                    break;
            }

            #endregion

            #region Delete-File

            if (success)
            {
                currTask.Completion = DateTime.Now;
                if (!Common.DeleteFile(file))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "TaskRunner unable to delete file " + file + ", marking task complete and storing");
                    if (!Common.WriteFile(file, Common.SerializeJson(currTask), false))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "TaskRunner unable to update file " + file + " to mark task complete, task will run again, correct manually");
                    }
                }
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "TaskRunner task in file " + file + " failed");
            }

            #endregion

            return;
        }

        #endregion
    }
}