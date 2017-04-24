using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using EmailWrapper;
using SyslogLogging;

namespace Kvpbase
{
    public class Events
    {
        #region Public-Members

        public EmailClient Email { get; set; }
        public LoggingModule Logging { get; set; }
        public Settings CurrentSettings { get; set; }

        #endregion

        #region Constructor

        public Events(Settings settings)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            CurrentSettings = settings;

            if (settings.Email != null)
            {
                switch (settings.Email.EmailProvider)
                {
                    case "smtp":
                        Email = new EmailClient(
                            settings.Email.SmtpServer,
                            settings.Email.SmtpPort,
                            settings.Email.SmtpUsername,
                            settings.Email.SmtpPassword,
                            Common.IsTrue(settings.Email.SmtpSsl));
                        break;

                    case "mailgun":
                        Email = new EmailClient(
                            settings.Mailgun.ApiKey,
                            settings.Mailgun.Domain);
                        break;
                }
            }
            else
            {
                Email = null;
            }

            if (settings.Syslog != null)
            {
                Logging = new LoggingModule(
                    settings.Syslog.ServerIp,
                    settings.Syslog.ServerPort,
                    Common.IsTrue(settings.Syslog.ConsoleLogging),
                    (LoggingModule.Severity)settings.Syslog.MinimumLevel,
                    false,
                    true,
                    true,
                    true,
                    true,
                    true);
            }
        }

        #endregion

        #region Public-Methods

        public void SendEmail(Email email)
        {
            if (Email == null) return;
            if (email == null) return;
            Email.Send(email);
        }

        public void DebugEmail(string subject, string content)
        {
            Email email = new Email();
            email.FromAddress = CurrentSettings.Email.EmailFrom;
            email.ToAddress = CurrentSettings.Email.EmailExceptionsTo;
            email.Body = content;
            email.ReplyAddress = CurrentSettings.Email.EmailReplyTo;
            email.Subject = subject;
            email.BccAddress = "";
            email.CcAddress = "";
            SendEmail(email);
        }

        public void Log(LoggingModule.Severity sev, string msg)
        {
            if (Logging == null) return;
            Logging.Log(sev, msg);
        }

        public void WebException(
           string url,
           string method,
           string filename,
           string text,
           string requestBody,
           WebException e)
        {
            string strResponse = "";
            if (e.Response != null)
            {
                try
                {
                    using (Stream stream = e.Response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            strResponse = reader.ReadToEnd();
                        }
                    }
                }
                catch (Exception)
                {
                }
            }
            
            Log(LoggingModule.Severity.Alert, Common.Line(79, "-"));
            Log(LoggingModule.Severity.Alert, "");
            Log(LoggingModule.Severity.Alert, "A WebException was encountered which triggered this message.");
            Log(LoggingModule.Severity.Alert, "Filename: " + filename);
            Log(LoggingModule.Severity.Alert, "Text: " + text);
            Log(LoggingModule.Severity.Alert, "Type: " + e.GetType().ToString());
            Log(LoggingModule.Severity.Alert, "");
            Log(LoggingModule.Severity.Alert, "The details of this exception are shown below:");
            Log(LoggingModule.Severity.Alert, "");
            Log(LoggingModule.Severity.Alert, "Type: WebException");
            Log(LoggingModule.Severity.Alert, "URL: " + url);
            Log(LoggingModule.Severity.Alert, "Method: " + method);
            Log(LoggingModule.Severity.Alert, "Data: " + e.Data);
            Log(LoggingModule.Severity.Alert, "Inner Exception: " + e.InnerException);
            Log(LoggingModule.Severity.Alert, "Message: " + e.Message);
            Log(LoggingModule.Severity.Alert, "Source: " + e.Source);
            Log(LoggingModule.Severity.Alert, "StackTrace: " + e.StackTrace);
            Log(LoggingModule.Severity.Alert, "Response: " + strResponse);
            Log(LoggingModule.Severity.Alert, "Request Body: " + requestBody);
            Log(LoggingModule.Severity.Alert, "Server: " + Dns.GetHostName());
            Log(LoggingModule.Severity.Alert, "");
            Log(LoggingModule.Severity.Alert, Common.Line(79, "-"));
            
            string body = "";
            
            body += "WebException on " + Dns.GetHostName() + " at " + DateTime.Now + " in " + filename + "\n\n";
            
            body += "A web exception was encountered on " + Dns.GetHostName() + " at " + DateTime.Now + " in " + filename + ".\n";
            body += "The details of the exception are below.\n\n";
            
            body += "Exception Detail\n";
            body += "  Exception Type: " + e.GetType().ToString() + "\n";
            body += "  URL: " + url + "\n";
            body += "  Method: " + method + "\n";
            body += "  Supplied Text: " + text + "\n";
            body += "  Exception Data: " + e.Data + "\n";
            body += "  Inner Exception: " + e.InnerException + "\n";
            body += "  Exception Message: " + e.Message + "\n";
            body += "  Exception Source: " + e.Source + "\n";
            body += "  Exception StackTrace: " + e.StackTrace + "\n\n";
            
            body += "Request Body\n";
            body += requestBody + "\n\n";
            
            body += "Response Body\n";
            body += strResponse + "\n\n";
            
            Email email = new Email();
            email.FromAddress = CurrentSettings.Email.EmailFrom;
            email.ToAddress = CurrentSettings.Email.EmailExceptionsTo;
            email.BccAddress = "";
            email.CcAddress = "";
            email.Subject = "WebException on " + Dns.GetHostName() + " at " + DateTime.Now + " in " + method;
            email.Body = body;
            email.ReplyAddress = CurrentSettings.Email.EmailReplyTo;
            email.IsHtml = false;
            SendEmail(email);

            return;
        }

        public void Exception(string method, string text, Exception e)
        {
            var st = new StackTrace(e, true);
            var frame = st.GetFrame(0);
            int fileLine = frame.GetFileLineNumber();
            string filename = frame.GetFileName();

            Log(LoggingModule.Severity.Alert, Common.Line(79, "-"));
            Log(LoggingModule.Severity.Alert, "An exception was encountered which triggered this message.");
            Log(LoggingModule.Severity.Alert, "Method: " + method);
            Log(LoggingModule.Severity.Alert, "Text: " + text);
            Log(LoggingModule.Severity.Alert, "Type: " + e.GetType().ToString());
            Log(LoggingModule.Severity.Alert, "");
            Log(LoggingModule.Severity.Alert, "Data: " + e.Data);
            Log(LoggingModule.Severity.Alert, "Inner: " + e.InnerException);
            Log(LoggingModule.Severity.Alert, "Message: " + e.Message);
            Log(LoggingModule.Severity.Alert, "Source: " + e.Source);
            Log(LoggingModule.Severity.Alert, "StackTrace: " + e.StackTrace);
            Log(LoggingModule.Severity.Alert, "Line: " + fileLine);
            Log(LoggingModule.Severity.Alert, "File: " + filename);
            Log(LoggingModule.Severity.Alert, "ToString: " + e.ToString());
            Log(LoggingModule.Severity.Alert, "(Servername: " + Dns.GetHostName());
            Log(LoggingModule.Severity.Alert, Common.Line(79, "-"));

            if (Common.IsTrue(CurrentSettings.Email.EmailExceptions))
            {
                Email email = new Email();
                email.FromAddress = CurrentSettings.Email.EmailExceptionsFrom;
                email.ToAddress = CurrentSettings.Email.EmailExceptionsTo;
                email.Body = ExceptionHtml(method, text, e);
                email.ReplyAddress = CurrentSettings.Email.EmailExceptionsReplyTo;
                email.Subject = "Exception on " + Dns.GetHostName() + " in " + method + " at " + DateTime.Now;
                email.BccAddress = "";
                email.CcAddress = "";
                email.IsHtml = true;
                SendEmail(email);
            }

            return;
        }

        public string ExceptionHtml(string method, string text, Exception e)
        {
            var st = new StackTrace(e, true);
            var frame = st.GetFrame(0);
            int fileLine = frame.GetFileLineNumber();
            string filename = frame.GetFileName();

            string html =
                "<html>" +
                " <head>" +
                "  <title>Exception on " + Dns.GetHostName() + " at " + DateTime.Now + "</title>" +
                " </head>" +
                " <body>" +
                "  <p>An exception was encountered on " + Dns.GetHostName() + " at " + DateTime.Now + " in method " + method + ".</p>" +
                "  <p>" +
                "   The details of the exception are as follows:<br />" +
                "   <ul>" +
                "    <li>Server name: " + Dns.GetHostName() + "</li>" +
                "    <li>File: " + filename + "</li>" +
                "    <li>Line Number: " + fileLine + "</li>" +
                "    <li>Method: " + method + "</li>" +
                "    <li>Text: " + text + " </li>" +
                "    <li>Type: " + e.GetType().ToString() + "</li>" +
                "    <li>Data: " + e.Data + "</li>" +
                "    <li>Inner: " + e.InnerException + "</li>" +
                "    <li>Message: " + e.Message + "</li>" +
                "    <li>Source: " + e.Source + "</li>" +
                "    <li>Stack Trace: " + e.StackTrace + "</li>" +
                "    <li>ToString: " + e.ToString() + "</li>" +
                "   </ul>" +
                "  </p>" +
                " </body>" +
                "</html>";

            return html;
        }

        #endregion

        #region Public-Static-Methods

        public static void ExceptionConsole(string method, string text, Exception e)
        {
            var st = new StackTrace(e, true);
            var frame = st.GetFrame(0);
            int fileLine = frame.GetFileLineNumber();
            string filename = frame.GetFileName();

            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("An exception was encountered which triggered this message.");
            Console.WriteLine("Method: " + method);
            Console.WriteLine("Text: " + text);
            Console.WriteLine("Type: " + e.GetType().ToString());
            Console.WriteLine("");
            Console.WriteLine("Data: " + e.Data);
            Console.WriteLine("Inner: " + e.InnerException);
            Console.WriteLine("Message: " + e.Message);
            Console.WriteLine("Source: " + e.Source);
            Console.WriteLine("StackTrace: " + e.StackTrace);
            Console.WriteLine("Line: " + fileLine);
            Console.WriteLine("File: " + filename);
            Console.WriteLine("ToString: " + e.ToString());
            Console.WriteLine("Servername: " + Dns.GetHostName());
            Console.WriteLine(Common.Line(79, "-"));
            return;
        }

        #endregion
    }
}