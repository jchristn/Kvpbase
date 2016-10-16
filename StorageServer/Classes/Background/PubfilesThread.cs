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
        public PublicObjThread(Settings settings, Events logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            Task.Run(() => PublicObjWorker(settings, logging));
        }

        private void PublicObjWorker(Settings settings, Events logging)
        {
            try
            {
                if (settings.PublicObj.RefreshSec <= 0)
                {
                    logging.Log(LoggingModule.Severity.Warn, "PublicObjWorker setting expiration processing timer to 60 sec (config value too low: " + settings.PublicObj.RefreshSec + " sec)");
                    settings.PublicObj.RefreshSec = 60;
                }
                
                logging.Log(LoggingModule.Severity.Debug, "PublicObjWorker starting with expiration processing timer set to " + settings.PublicObj.RefreshSec + " sec");

                bool firstRun = true;
                while (true)
                {
                    #region Wait

                    if (!firstRun)
                    {
                        Thread.Sleep(settings.PublicObj.RefreshSec * 1000);
                    }
                    else
                    {
                        firstRun = false;
                    }

                    #endregion

                    #region Ensure-Directory-Exists

                    if (!Common.DirectoryExists(settings.PublicObj.Directory))
                    {
                        if (!Common.CreateDirectory(settings.PublicObj.Directory))
                        {
                            logging.Log(LoggingModule.Severity.Warn, "PublicObjWorker unable to create missing directory " + settings.PublicObj.Directory);
                            continue;
                        }
                    }

                    #endregion

                    #region Retrieve-File-List

                    string[] files = Directory.GetFiles(settings.PublicObj.Directory);
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
                        DateTime compare = created.AddSeconds(settings.PublicObj.DefaultExpirationSec);
                        if (!Common.IsLaterThanNow(compare))
                        {
                            #region File-Expired

                            try
                            {
                                File.Delete(curr);
                                logging.Log(LoggingModule.Severity.Debug, "PublicObjWorker successfully cleaned up expired metadata file " + curr + " (created " + created.ToString("MM/dd/yyyy HH:mm:ss") + " expired " + compare.ToString("MM/dd/yyyy HH:mm:ss"));
                            }
                            catch (Exception)
                            {
                                logging.Log(LoggingModule.Severity.Warn, "PublicObjWorker unable to delete expired metadata file " + curr);
                            }

                            #endregion
                        }
                    }

                    #endregion
                }
            }
            catch (Exception e)
            {
                logging.Exception("PublicObjWorker", "Outer exception", e);
                Common.ExitApplication("PublicObjWorker", "Outer exception", -1);
                return;
            }
        }
    }
}