using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SyslogLogging;
using WatsonWebserver;
using RestWrapper;

namespace Kvpbase
{
    public class MessageManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private Events _Logging; 

        #endregion

        #region Constructors-and-Factories

        public MessageManager(Settings settings, Events logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging)); 

            _Settings = settings;
            _Logging = logging; 
        }

        #endregion

        #region Public-Methods

        public void Send(
            Node self,
            Node to,
            string subject,
            string data)
        {
            #region Variables

            Message curr = new Message();
            bool success = false;
            string req = "";

            #endregion

            #region Setup

            curr.From = self;
            curr.To = to;
            curr.Subject = subject;
            curr.Data = data;
            curr.Created = DateTime.Now;

            #endregion

            #region Set-URL

            string url = "";
            if (Common.IsTrue(curr.To.Ssl))
            {
                url = "https://" + curr.To.DnsHostname + ":" + curr.To.Port + "/admin/message";
            }
            else
            {
                url = "http://" + curr.To.DnsHostname + ":" + curr.To.Port + "/admin/message";
            }

            #endregion

            #region Attempt-to-Send

            req = Common.SerializeJson(curr);
            RestWrapper.RestResponse resp = RestRequest.SendRequestSafe(
                url, "application/json", "POST", null, null, false,
                Common.IsTrue(_Settings.Rest.AcceptInvalidCerts),
                Common.AddToDictionary(_Settings.Server.HeaderApiKey, _Settings.Server.AdminApiKey, null),
                Encoding.UTF8.GetBytes(req));

            if (resp == null)
            {
                #region No-REST-Response

                _Logging.Log(LoggingModule.Severity.Warn, "SendMessage null response connecting to " + url + ", message will be queued");
                success = false;

                #endregion
            }
            else
            {
                if (resp.StatusCode != 200)
                {
                    #region Failed-Message

                    _Logging.Log(LoggingModule.Severity.Warn, "SendMessage non-200 response connecting to " + url + ", message will be queued");
                    success = false;

                    #endregion
                }
                else
                {
                    #region Successful-Message

                    success = true;

                    #endregion
                }
            }

            #endregion

            #region Store-if-Needed

            if (!success)
            {
                #region Create-Directory-if-Needed

                if (!Common.DirectoryExists(_Settings.Messages.Directory + to.Name))
                {
                    try
                    {
                        Common.CreateDirectory(_Settings.Messages.Directory + to.Name);
                    }
                    catch (Exception e)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "SendMessage exception while creating directory " + _Settings.Messages.Directory + to.Name);
                        _Logging.Exception("SendMessage", "Exception while creating directory " + _Settings.Messages.Directory + to.Name, e);
                        return;
                    }
                }

                #endregion

                #region Generate-New-GUID

                int loopCount = 0;
                string guid = "";

                while (true)
                {
                    guid = Guid.NewGuid().ToString();
                    if (!Common.FileExists(_Settings.Messages.Directory + to.Name + Common.GetPathSeparator(_Settings.Environment) + guid))
                    {
                        break;
                    }

                    loopCount++;

                    if (loopCount > 16)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "SendMessage unable to generate unused GUID for folder " + _Settings.Messages.Directory + to.Name + ", exiting");
                        return;
                    }
                }

                #endregion

                #region Write-File

                if (!Common.WriteFile(
                    _Settings.Messages.Directory + to.Name + Common.GetPathSeparator(_Settings.Environment) + guid,
                    Common.SerializeJson(curr),
                    false))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendMessage unable to write message to " + _Settings.Messages.Directory + to.Name + Common.GetPathSeparator(_Settings.Environment) + guid + ", exiting");
                    return;
                }

                _Logging.Log(LoggingModule.Severity.Debug, "SendMessage queued message to " + _Settings.Messages.Directory + to.Name + Common.GetPathSeparator(_Settings.Environment) + guid);

                #endregion
            }

            #endregion

            return;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
