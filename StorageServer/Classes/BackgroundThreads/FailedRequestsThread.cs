using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EmailWrapper;
using SyslogLogging;

namespace Kvpbase.Classes.BackgroundThreads
{
    public class FailedRequestsThread
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private ConcurrentQueue<Dictionary<string, object>> _Queue;

        #endregion

        #region Constructors-and-Factories

        public FailedRequestsThread(Settings settings, LoggingModule logging, ConcurrentQueue<Dictionary<string, object>> queue)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            _Settings = settings;
            _Logging = logging;
            if (queue == null) _Queue = new ConcurrentQueue<Dictionary<string, object>>();
            else _Queue = queue;

            if (_Settings.Server.FailedRequestsIntervalSec < 60)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "FailedRequestsThread setting failed requests interval 60 sec (config value too low: " + _Settings.Server.FailedRequestsIntervalSec + " sec)");
                _Settings.Server.FailedRequestsIntervalSec = 60;
            }

            Task.Run(() => FailedRequestsWorker());
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        private void FailedRequestsWorker()
        { 
            #region Process

            while (true)
            {
                #region Wait

                Task.Delay(_Settings.Server.FailedRequestsIntervalSec * 1000).Wait();
                 
                if (_Queue == null)
                {
                    _Queue = new ConcurrentQueue<Dictionary<string, object>>();
                    continue;
                }

                #endregion

                #region Iterate

                List<Dictionary<string, object>> dicts = new List<Dictionary<string, object>>();
                Dictionary<string, object> curr;
                while (_Queue.TryDequeue(out curr))
                {
                    dicts.Add(curr);
                }

                if (dicts != null && dicts.Count > 0)
                {
                    #region Send-Email

                    _Logging.Log(LoggingModule.Severity.Debug, "FailedRequestsWorker sending details of " + dicts.Count + " failed requests");

                    Email email = new Email();
                    email.FromAddress = _Settings.Email.EmailExceptionsFrom;
                    email.ToAddress = _Settings.Email.EmailExceptionsTo;
                    email.Body = EmailBody(_Settings, dicts);
                    email.ReplyAddress = _Settings.Email.EmailExceptionsReplyTo;
                    email.Subject = "Failed requests on " + Dns.GetHostName();
                    email.BccAddress = "";
                    email.CcAddress = "";
                    email.IsHtml = true;

                    StorageServer.SendEmail(email);

                    #endregion
                }

                #endregion
            }

            #endregion
        }

        private string EmailBody(Settings settings, List<Dictionary<string, object>> dicts)
        {
            string ret = "";

            ret += EmailBuilder.Top("Failed Requests", false, null, null);
            ret += EmailBuilder.BigHeader("Failed Requests");
            ret += EmailBuilder.Paragraph("The following are requests that have received a non-200/201 response from server: " + Dns.GetHostName());
            ret += EmailBuilder.SmallHeader("Request Details");

            if (dicts == null || dicts.Count < 1)
            {
                ret += EmailBuilder.Paragraph("(null)");
            }
            else
            {
                foreach (Dictionary<string, object> curr in dicts)
                {
                    string para =
                        "<p>" +
                        "[" + curr["timestamp"] + "] Source " + curr["source"].ToString() + " => " + curr["dest"].ToString() + ": " + curr["method"].ToString() + " " + curr["raw_url_with_qs"].ToString() + "<br />" +
                        "<ul>";

                    if (curr.ContainsKey("headers"))
                    {
                        Dictionary<string, string> reqHeaders = (Dictionary<string, string>)curr["headers"];
                        if (reqHeaders != null && reqHeaders.Count > 0)
                        {
                            para += "<li>Headers:<br /><ul>";

                            foreach (KeyValuePair<string, string> currHeader in reqHeaders)
                            {
                                para += "<li>" + currHeader.Key + ": " + currHeader.Value + "</li>";
                            }

                            para += "</ul></li>";
                        }
                    }

                    if (curr.ContainsKey("status")) para += "<li>Status code: " + curr["status"].ToString() + "</li>";
                    if (curr.ContainsKey("request_body"))
                    {
                        if (curr["request_body"] != null)
                        {
                            if (curr["request_body"] is byte[])
                            {
                                para += "<li>Request body: " + Encoding.UTF8.GetString((byte[])curr["request_body"]) + "</li>";
                            }
                            else if (curr["request_body"] is string)
                            {
                                para += "<li>Request body: " + curr["request_body"].ToString() + "</li>";
                            }
                            else
                            {
                                para += "<li>Request body: (unknown type)</li>";
                            }
                        }
                    }

                    if (curr.ContainsKey("response_body"))
                    {
                        if (curr["response_body"] != null)
                        {
                            if (curr["response_body"] is byte[])
                            {
                                para += "<li>Response body: " + Encoding.UTF8.GetString((byte[])curr["response_body"]) + "</li>";
                            }
                            else if (curr["response_body"] is string)
                            {
                                para += "<li>Response body: " + curr["response_body"].ToString() + "</li>";
                            }
                            else
                            {
                                para += "<li>Response body: (unknown type)</li>";
                            }
                        }
                    }

                    para +=
                        "</ul>" +
                        "</p>";

                    ret += EmailBuilder.Paragraph(para);
                }
            }

            ret += EmailBuilder.Bottom(false, null);

            return ret;
        }

        #endregion
    }
}