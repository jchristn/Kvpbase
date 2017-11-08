using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;

namespace Kvpbase
{
    public class PublicObjThread
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private Events _Logging;

        #endregion

        #region Constructors-and-Factories

        public PublicObjThread(Settings settings, Events logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            _Settings = settings;
            _Logging = logging;

            if (_Settings.PublicObj.RefreshSec <= 0)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "PublicObjThread setting expiration processing timer to 60 sec, config value too low: " + _Settings.PublicObj.RefreshSec + " sec");
                _Settings.PublicObj.RefreshSec = 60;
            }

            Task.Run(() => PublicObjWorker());
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        private void PublicObjWorker()
        {
            try
            { 
                while (true)
                {
                    #region Wait

                    Task.Delay(_Settings.PublicObj.RefreshSec * 1000).Wait();

                    #endregion

                    #region Ensure-Directory-Exists

                    if (!Common.DirectoryExists(_Settings.PublicObj.Directory))
                    {
                        if (!Common.CreateDirectory(_Settings.PublicObj.Directory))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "PublicObjWorker unable to create missing directory " + _Settings.PublicObj.Directory);
                            continue;
                        }
                    }

                    #endregion

                    #region Retrieve-File-List

                    string[] files = Directory.GetFiles(_Settings.PublicObj.Directory);
                    if (files == null || files.Length < 1)
                    {
                        // logging.Log(LoggingModule.Severity.Debug, "PublicObjWorker no files found to process for expiration");
                        continue;
                    }

                    #endregion

                    #region Retrieve-Metadata

                    foreach (string curr in files)
                    {
                        DateTime created = File.GetCreationTime(curr);
                        DateTime compare = created.AddSeconds(_Settings.PublicObj.DefaultExpirationSec);
                        if (!Common.IsLaterThanNow(compare))
                        {
                            #region File-Expired

                            try
                            {
                                File.Delete(curr);
                                _Logging.Log(LoggingModule.Severity.Debug, "PublicObjWorker successfully cleaned up expired metadata file " + curr + " (created " + created.ToString("MM/dd/yyyy HH:mm:ss") + " expired " + compare.ToString("MM/dd/yyyy HH:mm:ss"));
                            }
                            catch (Exception)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "PublicObjWorker unable to delete expired metadata file " + curr);
                            }

                            #endregion
                        }
                    }

                    #endregion
                }
            }
            catch (Exception e)
            {
                _Logging.Exception("PublicObjWorker", "Outer exception", e);
                Common.ExitApplication("PublicObjWorker", "Outer exception", -1);
                return;
            }
        }

        #endregion
    }
}