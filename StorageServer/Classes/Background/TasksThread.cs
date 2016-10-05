using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;

namespace Kvpbase
{
    public class TasksThread
    {
        public TasksThread(Settings settings, Events logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            Task.Run(() => TasksWorker(settings, logging));
        }

        private void TasksWorker(Settings settings, Events logging)
        {
            #region Setup

            if (String.IsNullOrEmpty(settings.Tasks.Directory))
            {
                logging.Log(LoggingModule.Severity.Warn, "TasksWorker unable to open background task directory from configuration file");
                Common.ExitApplication("TasksWorker", "Undefined tasks directory", -1);
                return;
            }

            if (!Common.VerifyDirectoryAccess(settings.Environment, settings.Tasks.Directory))
            {
                logging.Log(LoggingModule.Severity.Warn, "TasksWorker unable to access background task directory " + settings.Tasks.Directory);
                Common.ExitApplication("TasksWorker", "Unable to access tasks directory", -1);
                return;
            }
            
            if (settings.Tasks.RefreshSec <= 0)
            {
                logging.Log(LoggingModule.Severity.Warn, "TasksWorker setting background task retry timer to 10 sec (config value too low: " + settings.Tasks.RefreshSec + " sec)");
                settings.Tasks.RefreshSec = 10;
            }

            logging.Log(LoggingModule.Severity.Debug, "TasksWorker starting with background task retry timer set to " + settings.Tasks.RefreshSec + " sec");

            #endregion

            #region Process

            bool firstRun = true;
            while (true)
            {
                #region Wait

                if (!firstRun)
                {
                    Thread.Sleep(settings.Tasks.RefreshSec * 1000);
                }
                else
                {
                    firstRun = false;
                }
                
                #endregion
                        
                #region Get-File-List

                List<string> tasks = new List<string>();
                tasks = Common.GetFileList(settings.Environment, settings.Tasks.Directory, false);

                if (tasks == null || tasks.Count < 1)
                {
                    continue;
                }

                #endregion

                #region Process-Each-File

                foreach (string currFile in tasks)
                {
                    #region Enumerate

                    logging.Log(LoggingModule.Severity.Debug, "TasksWorker processing task " + currFile);

                    #endregion

                    #region Read-File

                    string contents = Common.ReadTextFile(settings.Tasks.Directory + currFile);
                    if (String.IsNullOrEmpty(contents))
                    {
                        logging.Log(LoggingModule.Severity.Warn, "TasksWorker empty file detected at " + currFile + ", deleting");
                        if (!Common.DeleteFile(settings.Tasks.Directory + currFile))
                        {
                            logging.Log(LoggingModule.Severity.Warn, "TasksWorker unable to delete file " + currFile);
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
                            logging.Log(LoggingModule.Severity.Warn, "TasksWorker unable to deserialize file " + currFile + ", skipping");
                            continue;
                        }
                    }
                    catch (Exception)
                    {
                        logging.Log(LoggingModule.Severity.Warn, "TasksWorker unable unable to deserialize file " + currFile + ", skipping");
                        continue;
                    }

                    #endregion

                    #region Process

                    logging.Log(LoggingModule.Severity.Debug, "TasksWorker starting background job to process task " + currFile);
                    Task.Run(() => TaskRunner(settings, logging, settings.Tasks.Directory + currFile, currTask));

                    #endregion
                }

                #endregion
            }

            #endregion
        }

        private void TaskRunner(Settings settings, Events logging, string file, TaskObject currTask)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(file))
            {
                logging.Log(LoggingModule.Severity.Warn, "TaskRunner null value for file detected");
                return;
            }

            if (currTask == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "TaskRunner null value for curr_task detected");
                return;
            }

            #endregion

            #region Check-for-Completion

            if (currTask.Completion != null)
            {
                logging.Log(LoggingModule.Severity.Warn, "TaskRunner task already completed in file " + file + ", attempting to delete");
                Common.DeleteFile(file);
                return;
            }

            #endregion

            #region Process

            bool success = false;
            switch (currTask.TaskType)
            {
                case "loopback":
                    logging.Log(LoggingModule.Severity.Info, Common.Line(79, "-"));
                    logging.Log(LoggingModule.Severity.Info, "");
                    logging.Log(LoggingModule.Severity.Info, "Loopback task detected in file " + file);
                    logging.Log(LoggingModule.Severity.Info, "");
                    logging.Log(LoggingModule.Severity.Info, Common.Line(79, "-"));
                    success = true;
                    break;

                default:
                    logging.Log(LoggingModule.Severity.Warn, "TaskRunner unknown task type " + currTask.TaskType + " in file " + file);
                    break;
            }

            #endregion

            #region Delete-File

            if (success)
            {
                currTask.Completion = DateTime.Now;
                if (!Common.DeleteFile(file))
                {
                    logging.Log(LoggingModule.Severity.Warn, "TaskRunner unable to delete file " + file + ", marking task complete and storing");
                    if (!Common.WriteFile(file, Common.SerializeJson(currTask), false))
                    {
                        logging.Log(LoggingModule.Severity.Warn, "TaskRunner unable to update file " + file + " to mark task complete, task will run again, correct manually");
                    }
                }
            }
            else
            {
                logging.Log(LoggingModule.Severity.Warn, "TaskRunner task in file " + file + " failed");
            }

            #endregion

            return;
        }
    }
}