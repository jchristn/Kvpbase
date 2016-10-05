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

namespace Kvpbase
{
    public class FailedRequestsThread
    {
        public FailedRequestsThread(Settings settings, Events logging, ConcurrentQueue<Dictionary<string, object>> queue)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (queue == null) queue = new ConcurrentQueue<Dictionary<string, object>>();
            Task.Run(() => FailedRequestsWorker(settings, logging, queue));
        }

        private void FailedRequestsWorker(Settings settings, Events logging, ConcurrentQueue<Dictionary<string, object>> queue)
        {
            #region Setup

            if (queue == null) queue = new ConcurrentQueue<Dictionary<string, object>>();

            if (settings.Server.FailedRequestsIntervalSec < 60)
            {
                logging.Log(LoggingModule.Severity.Warn, "FailedRequestsWorker setting failed requests interval 60 sec (config value too low: " + settings.Server.FailedRequestsIntervalSec + " sec)");
                settings.Server.FailedRequestsIntervalSec = 60;
            }

            logging.Log(LoggingModule.Severity.Debug, "FailedRequestsWorker starting with failed requests interval set to " + settings.Server.FailedRequestsIntervalSec + " sec");

            #endregion

            #region Process

            while (true)
            {
                #region Wait

                Thread.Sleep(settings.Server.FailedRequestsIntervalSec * 1000);

                #endregion

                #region Check-for-Null-Values

                if (queue == null)
                {
                    queue = new ConcurrentQueue<Dictionary<string, object>>();
                    continue;
                }

                #endregion

                #region Iterate

                List<Dictionary<string, object>> dicts = new List<Dictionary<string, object>>();
                Dictionary<string, object> curr;
                while (queue.TryDequeue(out curr))
                {
                    dicts.Add(curr);
                }

                if (dicts != null && dicts.Count > 0)
                {
                    #region Send-Email

                    logging.Log(LoggingModule.Severity.Debug, "FailedRequestsWorker sending details of " + dicts.Count + " failed requests");

                    Email email = new Email();
                    email.FromAddress = settings.Email.EmailExceptionsFrom;
                    email.ToAddress = settings.Email.EmailExceptionsTo;
                    email.Body = EmailBody(settings, dicts);
                    email.ReplyAddress = settings.Email.EmailExceptionsReplyTo;
                    email.Subject = "Failed requests on " + Dns.GetHostName();
                    email.BccAddress = "";
                    email.CcAddress = "";
                    email.IsHtml = true;
                    logging.SendEmail(email);

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

            ret += EmailBuilder.Bottom(true, settings.LogoUrl, settings.HomepageUrl, false, null);

            return ret;
        }
    }
}