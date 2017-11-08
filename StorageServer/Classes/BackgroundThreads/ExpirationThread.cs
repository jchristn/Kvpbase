using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;

namespace Kvpbase
{
    public class ExpirationThread
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private Events _Logging;

        #endregion

        #region Constructors-and-Factories

        public ExpirationThread(Settings settings, Events logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            _Settings = settings;
            _Logging = logging;

            if (_Settings.Expiration == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ExpirationThread null expiration settings, exiting");
                return;
            }

            if (String.IsNullOrEmpty(_Settings.Expiration.Directory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ExpirationThread unable to open expiration queue directory from configuration file");
                Common.ExitApplication("ExpirationWorker", "Undefined expiration queue directory", -1);
                return;
            }

            if (!Common.VerifyDirectoryAccess(_Settings.Environment, settings.Expiration.Directory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ExpirationThread unable to access expiration queue directory " + settings.Expiration.Directory);
                Common.ExitApplication("ExpirationWorker", "Unable to access expiration queue directory", -1);
                return;
            }

            if (_Settings.Expiration.RefreshSec < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ExpirationThread setting expiration queue retry timer to 10 sec (config value too low: " + settings.Expiration.RefreshSec + " sec)");
                settings.Expiration.RefreshSec = 10;
            }

            Task.Run(() => ExpirationWorker(settings, logging));
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        private void ExpirationWorker(Settings settings, Events logging)
        { 
            #region Process
             
            while (true)
            {
                #region Wait
                 
                Task.Delay(_Settings.Expiration.RefreshSec * 1000).Wait();
                
                #endregion
                
                #region Variables

                List<string> expiredFiles = new List<string>();
                DateTime expirationTimestamp;
                string fileContents = "";
                Obj curr = new Obj();

                #endregion

                #region Get-Expiration-File-List

                expiredFiles = Common.GetFileList(_Settings.Environment, settings.Expiration.Directory, false);
                if (expiredFiles == null || expiredFiles.Count < 1)
                {
                    continue;
                }

                #endregion

                #region Process-Each-File

                foreach (string currFile in expiredFiles)
                {
                    #region Check-Filename-Length

                    // MMddyyyy-hhmmss-rrrrrrrr-originalname.ext
                    // 012345678901234

                    if (String.IsNullOrEmpty(currFile)) continue;
                    if (currFile.Length < 15)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ExpirationWorker filename " + currFile + " not at least 15 characters in length");
                        continue;
                    }

                    #endregion

                    #region Convert-Filename-to-Datetime

                    try
                    {
                        expirationTimestamp = DateTime.ParseExact(currFile.Substring(0, 15), "MMddyyyy-hhmmss", CultureInfo.InvariantCulture);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ExpirationWorker unable to convert filename " + currFile + " to a timestamp for comparison, skipping");
                        continue;
                    }

                    if (Common.IsLaterThanNow(expirationTimestamp))
                    {
                        continue;
                    }

                    #endregion

                    #region Read-File

                    fileContents = Common.ReadTextFile(_Settings.Expiration.Directory + currFile);
                    if (String.IsNullOrEmpty(fileContents))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ExpirationWorker unable to read file " + settings.Expiration.Directory + currFile);
                        continue;
                    }

                    #endregion

                    #region Deserialize

                    try
                    {
                        // expiration objects are always serialized JSON
                        curr = Common.DeserializeJson<Obj>(fileContents);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ExpirationWorker unable to deserialize contents from file " + settings.Expiration.Directory + currFile);
                        continue;
                    }

                    #endregion

                    #region Check-Existence

                    if (!Common.FileExists(curr.DiskPath))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "ExpirationWorker file referenced in " + currFile + " no longer exists: " + curr.DiskPath);
                        if (!Common.DeleteFile(_Settings.Expiration.Directory + currFile))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ExpirationWorker unable to delete expiration job file " + currFile);
                            continue;
                        }
                    }

                    #endregion

                    #region Delete

                    if (!Common.DeleteFile(curr.DiskPath))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ExpirationWorker unable to delete file referenced in " + currFile + ": " + curr.DiskPath);
                        continue;
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "ExpirationWorker successfully deleted expired file " + curr.DiskPath);
                        if (!Common.DeleteFile(_Settings.Expiration.Directory + currFile))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ExpirationWorker unable to delete expiration job file " + currFile);
                            continue;
                        }
                    }

                    #endregion
                }

                #endregion
            }

            #endregion
        }

        #endregion
    }
}