using System;
using System.Text;
using SyslogLogging;
using RestWrapper;

namespace Kvpbase
{
    public class Message
    {
        #region Public-Members

        public Node From { get; set; }
        public Node To { get; set; }
        public string Subject { get; set; }
        public string Data { get; set; }
        public DateTime? Created { get; set; }

        #endregion

        #region Constructors-and-Factories

        public Message()
        {

        }

        #endregion

        #region Static-Methods

        public static void SendMessage(
            Settings settings,
            Events logging,
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
                Common.IsTrue(settings.Rest.AcceptInvalidCerts),
                Common.AddToDictionary(settings.Server.HeaderApiKey, settings.Server.AdminApiKey, null),
                Encoding.UTF8.GetBytes(req));

            if (resp == null)
            {
                #region No-REST-Response

                logging.Log(LoggingModule.Severity.Warn, "SendMessage null response connecting to " + url + ", message will be queued");
                success = false;

                #endregion
            }
            else
            {
                if (resp.StatusCode != 200)
                {
                    #region Failed-Message

                    logging.Log(LoggingModule.Severity.Warn, "SendMessage non-200 response connecting to " + url + ", message will be queued");
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

                if (!Common.DirectoryExists(settings.Messages.Directory + to.Name))
                {
                    try
                    {
                        Common.CreateDirectory(settings.Messages.Directory + to.Name);
                    }
                    catch (Exception e)
                    {
                        logging.Log(LoggingModule.Severity.Warn, "SendMessage exception while creating directory " + settings.Messages.Directory + to.Name);
                        logging.Exception("SendMessage", "Exception while creating directory " + settings.Messages.Directory + to.Name, e);
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
                    if (!Common.FileExists(settings.Messages.Directory + to.Name + Common.GetPathSeparator(settings.Environment) + guid))
                    {
                        break;
                    }

                    loopCount++;

                    if (loopCount > 16)
                    {
                        logging.Log(LoggingModule.Severity.Warn, "SendMessage unable to generate unused GUID for folder " + settings.Messages.Directory + to.Name + ", exiting");
                        return;
                    }
                }

                #endregion

                #region Write-File

                if (!Common.WriteFile(
                    settings.Messages.Directory + to.Name + Common.GetPathSeparator(settings.Environment) + guid,
                    Common.SerializeJson(curr),
                    false))
                {
                    logging.Log(LoggingModule.Severity.Warn, "SendMessage unable to write message to " + settings.Messages.Directory + to.Name + Common.GetPathSeparator(settings.Environment) + guid + ", exiting");
                    return;
                }

                logging.Log(LoggingModule.Severity.Debug, "SendMessage queued message to " + settings.Messages.Directory + to.Name + Common.GetPathSeparator(settings.Environment) + guid);

                #endregion
            }

            #endregion

            return;
        }

        #endregion
    }
}
