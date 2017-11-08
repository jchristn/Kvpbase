using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using RestWrapper;

namespace Kvpbase
{
    public class MessengerThread
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private Events _Logging;
        private Topology _Topology;
        private Node _Node;

        #endregion

        #region Constructors-and-Factories

        public MessengerThread(Settings settings, Events logging, Topology topology, Node self)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (self == null) throw new ArgumentNullException(nameof(self));

            _Settings = settings;
            _Logging = logging;
            _Topology = topology;
            _Node = self;

            if (_Topology == null || _Topology.IsEmpty())
            {
                _Logging.Log(LoggingModule.Severity.Debug, "MessengerThread exiting, no topology");
                return;
            }

            if (String.IsNullOrEmpty(_Settings.Messages.Directory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MessengerThread unable to open message queue directory from configuration file");
                Common.ExitApplication("MessengerThread", "Undefined message queue directory", -1);
                return;
            }

            if (!Common.VerifyDirectoryAccess(_Settings.Environment, settings.Messages.Directory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MessengerThread unable to access message queue directory " + settings.Messages.Directory);
                Common.ExitApplication("MessengerThread", "Unable to access message queue directory", -1);
                return;
            }

            if (_Settings.Messages.RefreshSec <= 0)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MessengerThread setting message queue retry timer to 10 sec, config value too low: " + settings.Messages.RefreshSec + " sec");
                _Settings.Messages.RefreshSec = 10;
            }

            Task.Run(() => MessengerWorker());
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        private void MessengerWorker()
        { 
            #region Process

            while (true)
            {
                #region Wait

                Task.Delay(this._Settings.Messages.RefreshSec * 1000).Wait();
                 
                #endregion

                #region Get-Subdirectory-List

                List<string> subdirectories = new List<string>();
                subdirectories = Common.GetSubdirectoryList(this._Settings.Messages.Directory, true);

                if (subdirectories == null || subdirectories.Count < 1) continue;

                #endregion

                #region Process-Each-Subdirectory

                foreach (string subdirectory in subdirectories)
                {
                    #region Enumerate

                    // logging.Log(LoggingModule.Severity.Debug, "MessengerWorker processing directory " + subdirectory);

                    #endregion

                    #region Get-File-List

                    List<string> files = Common.GetFileList(this._Settings.Environment, subdirectory, false);
                    if (files == null) continue;
                    if (files.Count < 1) continue;

                    #endregion

                    #region Process-Each-File

                    foreach (string file in files)
                    {
                        #region Enumerate

                        // logging.Log(LoggingModule.Severity.Debug, "MessengerWorker processing file " + subdirectory + file);

                        #endregion

                        #region Read-File

                        string contents = Common.ReadTextFile(subdirectory + Common.GetPathSeparator(this._Settings.Environment) + file);
                        if (String.IsNullOrEmpty(contents))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "MessengerWorker empty file detected at " + subdirectory + Common.GetPathSeparator(this._Settings.Environment) + file + ", deleting");
                            if (!Common.DeleteFile(subdirectory + file))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "MessengerWorker unable to delete file " + subdirectory + Common.GetPathSeparator(this._Settings.Environment) + file);
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
                                _Logging.Log(LoggingModule.Severity.Warn, "MessengerWorker unable to deserialize file " + subdirectory + file + ", skipping");
                                continue;
                            }
                        }
                        catch (Exception)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "MessengerWorker unable to deserialize file " + subdirectory + file + ", skipping");
                            continue;
                        }

                        #endregion

                        #region Set-Message-Parameters

                        currMessage.From = _Node;

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
                            Common.IsTrue(this._Settings.Rest.AcceptInvalidCerts),
                            Common.AddToDictionary(this._Settings.Server.HeaderApiKey, _Settings.Server.AdminApiKey, null),
                            Encoding.UTF8.GetBytes(req));

                        if (resp == null)
                        {
                            #region No-REST-Response

                            _Logging.Log(LoggingModule.Severity.Warn, "MessengerWorker null response connecting to " + url + ", message " + subdirectory + Common.GetPathSeparator(this._Settings.Environment) + file + " will remain queued");
                            continue;

                            #endregion
                        }
                        else
                        {
                            if (resp.StatusCode != 200)
                            {
                                #region Failed-Message

                                _Logging.Log(LoggingModule.Severity.Warn, "MessengerWorker non-200 response connecting to " + url + ", message " + subdirectory + file + " will remain queued");
                                continue;

                                #endregion
                            }
                            else
                            {
                                #region Successful-Message

                                _Logging.Log(LoggingModule.Severity.Debug, "MessengerWorker successfully sent message " + subdirectory + Common.GetPathSeparator(this._Settings.Environment) + file);

                                if (!Common.DeleteFile(subdirectory + Common.GetPathSeparator(this._Settings.Environment) + file))
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "MessengerWorker unable to delete file " + subdirectory + Common.GetPathSeparator(this._Settings.Environment) + file);
                                }
                                else
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "MessengerWorker successfully deleted file " + subdirectory + Common.GetPathSeparator(this._Settings.Environment) + file);
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