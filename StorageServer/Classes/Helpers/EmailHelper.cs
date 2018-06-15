using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;
using WatsonWebserver;
using SyslogLogging;
using EmailWrapper;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static void SendDebugEmail(string subject, string content)
        {
            Email email = new Email();
            email.FromAddress = _Settings.Email.EmailFrom;
            email.ToAddress = _Settings.Email.EmailExceptionsTo;
            email.Body = content;
            email.ReplyAddress = _Settings.Email.EmailReplyTo;
            email.Subject = subject;
            email.BccAddress = "";
            email.CcAddress = "";
            SendEmail(email);
            return;
        }

        public static bool SendEmail(Email email)
        {
            if (email == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendEmail null email object supplied");
                return false;
            }

            switch (_Settings.Email.EmailProvider.ToLower())
            {
                case "mailgun":
                    MailgunSend(email);
                    break;

                case "smtp":
                    SendEmailSmtp(email);
                    break;

                default:
                    _Logging.Log(LoggingModule.Severity.Warn, "SendEmail unknown email provider in settings file: " + _Settings.Email.EmailProvider);
                    return false;
            }

            return true;
        }

        private static bool SendEmailSmtp(Email email)
        {
            // comma-separated list of email addresses
            if ((String.IsNullOrEmpty(email.FromAddress)) ||
                (String.IsNullOrEmpty(email.ToAddress)) ||
                (String.IsNullOrEmpty(email.Body)) ||
                (String.IsNullOrEmpty(email.ReplyAddress)) ||
                (String.IsNullOrEmpty(email.Subject)))
            {
                _Logging.Log(LoggingModule.Severity.Error, "SendEmailSmtp email object has empty fields");
                return false;
            }

            MailMessage msg = new MailMessage();
            msg.From = new MailAddress(email.FromAddress);
            msg.To.Add(email.ToAddress);

            if (String.Compare(String.Empty, email.CcAddress) != 0)
            {
                msg.CC.Add(email.CcAddress);
            }
            if (String.Compare(String.Empty, email.BccAddress) != 0)
            {
                msg.Bcc.Add(email.BccAddress);
            }

            if (!String.IsNullOrEmpty(email.ReplyAddress)) msg.ReplyToList.Add(email.ReplyAddress);
            msg.Subject = Dns.GetHostName() + ": " + email.Subject;
            msg.Body = email.Body;

            string smtpServer = _Settings.Email.SmtpServer;
            int smtpPort = _Settings.Email.SmtpPort;
            string smtpUser = _Settings.Email.SmtpUsername;
            string smtpPass = _Settings.Email.SmtpPassword;

            SmtpClient smtpClient = new SmtpClient(smtpServer, smtpPort);
            smtpClient.UseDefaultCredentials = false;
            smtpClient.EnableSsl = Common.IsTrue(_Settings.Email.SmtpSsl);
            smtpClient.Credentials = new NetworkCredential(smtpUser, smtpPass, null);

            if (email.IsHtml) msg.IsBodyHtml = true;
            else msg.IsBodyHtml = false;

            if (!String.IsNullOrEmpty(email.AttachmentData))
            {
                byte[] attachmentBytes = Encoding.UTF8.GetBytes(email.AttachmentData);
                MemoryStream attachmenStream = new MemoryStream(attachmentBytes);
                msg.Attachments.Add(new Attachment(attachmenStream, email.AttachmentName, email.AttachmentContentType));
                attachmenStream.Dispose();
            }

            try
            {
                smtpClient.Send(msg);
            }
            catch (Exception)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendEmailSmtp exception encountered while attempting to send email");
                return false;
            }

            return true;
        }
    }
}