using System;
using RestSharp;
using RestSharp.Authenticators;
using System.Net;
using System.Text;
using System.Web;
using SyslogLogging;
using EmailWrapper;

namespace Kvpbase
{
    public static partial class StorageServer
    {
        public static bool MailgunSend(Email email)
        {
            try
            {
                RestClient client = new RestClient();
                client.BaseUrl = new Uri("https://api.mailgun.net/v3");
                client.Authenticator = new HttpBasicAuthenticator("api", _Settings.Mailgun.ApiKey);
                RestRequest request = new RestRequest();
                request.AddParameter("domain", _Settings.Mailgun.Domain, ParameterType.UrlSegment);
                request.Resource = _Settings.Mailgun.Domain + "/" + _Settings.Mailgun.ResourceSendmessage;
                request.AddParameter("from", email.FromAddress);
                request.AddParameter("to", email.ToAddress);

                if (!String.IsNullOrEmpty(email.CcAddress)) request.AddParameter("cc", email.CcAddress);
                if (!String.IsNullOrEmpty(email.BccAddress)) request.AddParameter("bcc", email.BccAddress);

                request.AddParameter("subject", email.Subject);

                if (email.IsHtml) request.AddParameter("html", email.Body);
                else request.AddParameter("text", email.Body);

                if (!String.IsNullOrEmpty(email.AttachmentData))
                    request.AddFileBytes(
                        "attachment",
                        Encoding.UTF8.GetBytes(email.AttachmentData),
                        email.AttachmentName,
                        email.AttachmentContentType);

                request.Method = Method.POST;
                IRestResponse response = client.Execute(request);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "MailgunSend successfully sent message to " + email.ToAddress + " from " + email.FromAddress);
                    return true;
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "MailgunSend message to " + email.ToAddress + " not sent successfully: " + response.StatusCode.ToString());
                    return false;
                }
            }
            catch (Exception e)
            {
                _Logging.LogException("MailgunSend", "exception encountered when attempting to send email", e);
                return false;
            }
        }
    }
}