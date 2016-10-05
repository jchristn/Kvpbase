using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;

namespace Kvpbase
{
    public class LoggerManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private ConcurrentQueue<Tuple<string, string>> LoggerQueue;

        #endregion

        #region Constructors-and-Factories

        public LoggerManager()
        {

        }

        public LoggerManager(Settings settings, Events logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            LoggerQueue = new ConcurrentQueue<Tuple<string, string>>();

            Task.Run(() => LoggerWorker(settings, logging, LoggerQueue));
        }

        #endregion

        #region Public-Methods

        public void Add(string logfile, string contents)
        {
            DateTime utc = DateTime.Now.ToUniversalTime();
            string datestr = utc.Date.ToString("MMddyyyy");
            string timestr = utc.TimeOfDay.ToString("hhmmss");

            contents = datestr + "," + timestr + "," + contents;
            LoggerQueue.Enqueue(new Tuple<string, string>(logfile, contents));
        }
        
        #endregion

        #region Private-Methods

        private void LoggerWorker(Settings settings, Events logging, ConcurrentQueue<Tuple<string, string>> queue)
        {
            #region Setup

            if (settings.Topology.RefreshSec <= 0)
            {
                logging.Log(LoggingModule.Severity.Warn, "LoggerWorker setting topology refresh timer to 10 sec (config value too low: " + settings.Topology.RefreshSec + " sec)");
                settings.Topology.RefreshSec = 10;
            }

            logging.Log(LoggingModule.Severity.Debug, "LoggerWorker starting with topology refresh timer set to " + settings.Topology.RefreshSec + " sec");

            if (settings.Logger == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "LoggerWorker null logger section in config file, exiting");
                return;
            }

            if (settings.Logger.RefreshSec < 10)
            {
                logging.Log(LoggingModule.Severity.Warn, "LoggerWorker invalid value for refresh interval, using default of 10");
                settings.Topology.RefreshSec = 10;
            }

            #endregion

            #region Process

            bool firstRun = true;
            while (true)
            {
                #region Wait

                if (!firstRun)
                {
                    Thread.Sleep(settings.Topology.RefreshSec * 1000);
                }
                else
                {
                    firstRun = false;
                }
                
                #endregion

                #region Process

                if (queue != null)
                {
                    Tuple<string, string> message;
                    while (queue.TryDequeue(out message))
                    {
                        string logfile = String.Copy(message.Item1);
                        string contents = String.Copy(message.Item2);

                        if (!Common.WriteFile(logfile, contents, true))
                        {
                            logging.Log(LoggingModule.Severity.Warn, "LoggerWorker unable to append the following message to " + logfile);
                            logging.Log(LoggingModule.Severity.Warn, contents);
                        }
                    }
                }

                #endregion
            }

            #endregion
        }

        #endregion

        #region Public-Static-Methods

        public static string BuildMessage(RequestMetadata md, string operation, string text)
        {
            // src_ip,UserMasterId,user_guid,ApiKeyId,api_key_guid,operation(RCD)

            string ret = "";

            if (md != null)
            {
                if (md.CurrentHttpRequest != null)
                {
                    ret += md.CurrentHttpRequest.SourceIp + ",";
                }
                else
                {
                    ret += "0.0.0.0,";
                }
            }
            else
            {
                ret += "0.0.0.0,";
            }

            if (md.CurrentUserMaster != null)
            {
                ret += md.CurrentUserMaster.UserMasterId + "," + md.CurrentUserMaster.Guid + ",";
            }
            else
            {
                ret += "0,,";
            }

            if (md.CurrentApiKey != null)
            {
                ret += md.CurrentApiKey.ApiKeyId + "," + md.CurrentApiKey.Guid + ",";
            }
            else
            {
                ret += "0,,";
            }

            ret += operation + "," + text;
            return ret;
        }

        #endregion
    }
}