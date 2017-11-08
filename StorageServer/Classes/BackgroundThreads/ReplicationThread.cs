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
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private Events _Logging;
        private Topology _Topology;
        private Node _Self;

        #endregion

        #region Constructors-and-Factories

        public ReplicationThread(Settings settings, Events logging, Topology topology, Node self)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (self == null) throw new ArgumentNullException(nameof(self));

            _Settings = settings;
            _Logging = logging;
            _Topology = topology;
            _Self = self;

            if (_Settings.Replication.RefreshSec <= 0)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ReplicationThread setting replication timer to 10 sec, config value too low: " + _Settings.Replication.RefreshSec + " sec");
                _Settings.Replication.RefreshSec = 10;
            }

            Task.Run(() => ReplicationWorker());
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        private void ReplicationWorker()
        { 
            #region Process
             
            while (true)
            {
                #region Wait

                Task.Delay(_Settings.Replication.RefreshSec * 1000).Wait();
                    
                #endregion
                    
                #region Get-Subdirectory-List

                List<string> subdirectories = new List<string>();
                subdirectories = Common.GetSubdirectoryList(_Settings.Replication.Directory, true);

                if (subdirectories == null || subdirectories.Count < 1)
                {
                    continue;
                }

                #endregion

                #region Process-Each-Subdirectory

                foreach (string subdirectory in subdirectories)
                {
                    #region Get-File-List

                    List<string> files = Common.GetFileList(_Settings.Environment, subdirectory, false);
                    if (files == null) continue;
                    if (files.Count < 1) continue;

                    #endregion

                    #region Process-Each-File

                    foreach (string file in files)
                    {
                        #region Read-File

                        string contents = Common.ReadTextFile(subdirectory + Common.GetPathSeparator(_Settings.Environment) + file);
                        if (String.IsNullOrEmpty(contents))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker empty file detected at " + subdirectory + Common.GetPathSeparator(_Settings.Environment) + file + ", deleting");
                            if (!Common.DeleteFile(subdirectory + file))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker unable to delete file " + subdirectory + Common.GetPathSeparator(_Settings.Environment) + file);
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
                                _Logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker unable to deserialize file " + subdirectory + file + ", skipping");
                                continue;
                            }
                        }
                        catch (Exception)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker unable to deserialize file " + subdirectory + file + ", skipping");
                            continue;
                        }

                        #endregion

                        #region Set-Message-Parameters

                        currMessage.From = _Self;

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
                            Common.IsTrue(_Settings.Rest.AcceptInvalidCerts),
                            Common.AddToDictionary(_Settings.Server.HeaderApiKey, _Settings.Server.AdminApiKey, null),
                            Encoding.UTF8.GetBytes(req));

                        if (resp == null)
                        {
                            #region No-REST-Response

                            _Logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker null response connecting to " + url + ", message " + subdirectory + file + " will remain queued");
                            continue;

                            #endregion
                        }
                        else
                        {
                            if (resp.StatusCode != 200)
                            {
                                #region Failed-Message

                                _Logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker non-200 response connecting to " + url + ", message " + subdirectory + file + " will remain queued");
                                continue;

                                #endregion
                            }
                            else
                            {
                                #region Successful-Message

                                _Logging.Log(LoggingModule.Severity.Debug, "ReplicationWorker successfully sent message " + subdirectory + Common.GetPathSeparator(_Settings.Environment) + file);

                                if (!Common.DeleteFile(subdirectory + Common.GetPathSeparator(_Settings.Environment) + file))
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ReplicationWorker unable to delete file " + subdirectory + Common.GetPathSeparator(_Settings.Environment) + file);
                                }
                                else
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "ReplicationWorker successfully deleted file " + subdirectory + Common.GetPathSeparator(_Settings.Environment) + file);
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

        #endregion
    }
}