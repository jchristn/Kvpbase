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

        private Settings _Settings;
        private Events _Logging;
        private ConcurrentQueue<Tuple<string, string>> _LoggerQueue;

        #endregion

        #region Constructors-and-Factories

        public LoggerManager()
        {

        }

        public LoggerManager(Settings settings, Events logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            _Settings = settings;
            _Logging = logging;
            _LoggerQueue = new ConcurrentQueue<Tuple<string, string>>();

            Task.Run(() => LoggerWorker());
        }

        #endregion

        #region Public-Methods

        public void Add(string logfile, string contents)
        {
            DateTime utc = DateTime.Now.ToUniversalTime();
            string datestr = utc.Date.ToString("MMddyyyy");
            string timestr = utc.TimeOfDay.ToString("hhmmss");

            contents = datestr + "," + timestr + "," + contents;
            _LoggerQueue.Enqueue(new Tuple<string, string>(logfile, contents));
        }

        public static string BuildMessage(RequestMetadata md, string operation, string text)
        {
            // src_ip,UserMasterId,user_guid,ApiKeyId,api_key_guid,operation(RCD)

            string ret = "";

            if (md != null)
            {
                if (md.CurrHttpReq != null)
                {
                    ret += md.CurrHttpReq.SourceIp + ",";
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

            if (md.CurrUser != null)
            {
                ret += md.CurrUser.UserMasterId + "," + md.CurrUser.Guid + ",";
            }
            else
            {
                ret += "0,,";
            }

            if (md.CurrApiKey != null)
            {
                ret += md.CurrApiKey.ApiKeyId + "," + md.CurrApiKey.Guid + ",";
            }
            else
            {
                ret += "0,,";
            }

            ret += operation + "," + text;
            return ret;
        }

        #endregion

        #region Private-Methods

        private void LoggerWorker()
        {
            #region Setup

            if (_Settings.Topology.RefreshSec <= 0)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "LoggerWorker setting topology refresh timer to 10 sec (config value too low: " + _Settings.Topology.RefreshSec + " sec)");
                _Settings.Topology.RefreshSec = 10;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "LoggerWorker starting with topology refresh timer set to " + _Settings.Topology.RefreshSec + " sec");

            if (_Settings.Logger == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "LoggerWorker null logger section in config file, exiting");
                return;
            }

            if (_Settings.Logger.RefreshSec < 10)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "LoggerWorker invalid value for refresh interval, using default of 10");
                _Settings.Topology.RefreshSec = 10;
            }

            #endregion

            #region Process
             
            while (true)
            {
                #region Wait

                Task.Delay(_Settings.Topology.RefreshSec * 1000).Wait();
                
                #endregion

                #region Process

                if (_LoggerQueue != null)
                {
                    Tuple<string, string> message;
                    while (_LoggerQueue.TryDequeue(out message))
                    {
                        string logfile = String.Copy(message.Item1);
                        string contents = String.Copy(message.Item2);

                        if (!Common.WriteFile(logfile, contents, true))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "LoggerWorker unable to append the following message to " + logfile);
                            _Logging.Log(LoggingModule.Severity.Warn, contents);
                        }
                    }
                }

                #endregion
            }

            #endregion
        }

        #endregion 
    }
}