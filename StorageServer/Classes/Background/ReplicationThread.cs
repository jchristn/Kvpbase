using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using RestWrapper;

namespace Kvpbase
{
    public class ReplicationThread
    {
        public ReplicationThread(Settings settings, Events logging, Topology topology, Node self)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (self == null) throw new ArgumentNullException(nameof(self));
            Task.Run(() => ReplicationWorker(settings, logging, topology, self));
        }

        private void ReplicationWorker(Settings settings, Events logging, Topology topology, Node self)
        {
            #region Setup

            if (settings.Replication.RefreshSec <= 0)
            {
                logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker setting replication timer to 10 sec (config value too low: " + settings.Replication.RefreshSec + " sec)");
                settings.Replication.RefreshSec = 10;
            }

            logging.Log(LoggingModule.Severity.Debug, "ReplicationWorker starting with replication timer set to " + settings.Replication.RefreshSec + " sec");

            #endregion

            #region Process

            bool firstRun = true;
            while (true)
            {
                #region Wait

                if (!firstRun)
                {
                    Thread.Sleep(settings.Replication.RefreshSec * 1000);
                }
                else
                {
                    firstRun = false;
                }
                    
                #endregion
                    
                #region Get-Subdirectory-List

                List<string> subdirectories = new List<string>();
                subdirectories = Common.GetSubdirectoryList(settings.Replication.Directory, true);

                if (subdirectories == null || subdirectories.Count < 1)
                {
                    continue;
                }

                #endregion

                #region Process-Each-Subdirectory

                foreach (string subdirectory in subdirectories)
                {
                    #region Get-File-List

                    List<string> files = Common.GetFileList(settings.Environment, subdirectory, false);
                    if (files == null) continue;
                    if (files.Count < 1) continue;

                    #endregion

                    #region Process-Each-File

                    foreach (string file in files)
                    {
                        #region Read-File

                        string contents = Common.ReadTextFile(subdirectory + Common.GetPathSeparator(settings.Environment) + file);
                        if (String.IsNullOrEmpty(contents))
                        {
                            logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker empty file detected at " + subdirectory + Common.GetPathSeparator(settings.Environment) + file + ", deleting");
                            if (!Common.DeleteFile(subdirectory + file))
                            {
                                logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker unable to delete file " + subdirectory + Common.GetPathSeparator(settings.Environment) + file);
                                continue;
                            }
                        }

                        #endregion

                        #region Deserialize

                        Message currMessage = null;
                        try
                        {
                            currMessage = Common.DeserializeJson<Message>(contents);
                            if (currMessage == null)
                            {
                                logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker unable to deserialize file " + subdirectory + file + ", skipping");
                                continue;
                            }
                        }
                        catch (Exception)
                        {
                            logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker unable to deserialize file " + subdirectory + file + ", skipping");
                            continue;
                        }

                        #endregion

                        #region Set-Message-Parameters

                        currMessage.From = self;

                        string url = "";
                        if (Common.IsTrue(currMessage.To.Ssl))
                        {
                            url = "https://" + currMessage.To.DnsHostname + ":" + currMessage.To.Port + "/admin/message";
                        }
                        else
                        {
                            url = "http://" + currMessage.To.DnsHostname + ":" + currMessage.To.Port + "/admin/message";
                        }

                        #endregion

                        #region Send-Message

                        string req = Common.SerializeJson(currMessage);
                        RestWrapper.RestResponse resp = RestRequest.SendRequestSafe(
                            url, "application/json", "POST", null, null, false,
                            Common.IsTrue(settings.Rest.AcceptInvalidCerts),
                            Common.AddToDictionary(settings.Server.HeaderApiKey, settings.Server.AdminApiKey, null),
                            Encoding.UTF8.GetBytes(req));

                        if (resp == null)
                        {
                            #region No-REST-Response

                            logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker null response connecting to " + url + ", message " + subdirectory + file + " will remain queued");
                            continue;

                            #endregion
                        }
                        else
                        {
                            if (resp.StatusCode != 200)
                            {
                                #region Failed-Message

                                logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker non-200 response connecting to " + url + ", message " + subdirectory + file + " will remain queued");
                                continue;

                                #endregion
                            }
                            else
                            {
                                #region Successful-Message

                                logging.Log(LoggingModule.Severity.Debug, "ReplicationWorker successfully sent message " + subdirectory + Common.GetPathSeparator(settings.Environment) + file);

                                if (!Common.DeleteFile(subdirectory + Common.GetPathSeparator(settings.Environment) + file))
                                {
                                    logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker unable to delete file " + subdirectory + Common.GetPathSeparator(settings.Environment) + file);
                                }
                                else
                                {
                                    logging.Log(LoggingModule.Severity.Debug, "ReplicationWorker successfully deleted file " + subdirectory + Common.GetPathSeparator(settings.Environment) + file);
                                }

                                continue;

                                #endregion
                            }
                        }

                        #endregion
                    }

                    #endregion
                }

                #endregion
            }

            #endregion
        }
    }
}