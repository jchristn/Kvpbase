using System;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

namespace Kvpbase
{
    public class Find
    {
        #region Public-Members

        public string UserGuid { get; set; }
        public string Key { get; set; }
        public bool QueryTopology { get; set; }
        public bool Recursive { get; set; }
        public List<string> ContainerPath { get; set; }
        public List<SearchFilter> Filters { get; set; }
        public List<string> Urls { get; set; }

        #endregion

        #region Constructors-and-Factories

        public Find()
        {

        }

        #endregion

        #region Public-Static-Methods

        public static string BuildDiskPath(Find req, UserManager users, Settings settings, Events logging)
        {
            #region Check-for-Null-Values

            if (req == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath null find object supplied");
                return null;
            }

            #endregion

            #region Variables

            UserMaster currUser = new UserMaster();
            string homeDirectory = "";
            string fullPath = "";

            #endregion

            #region Get-User-Master-and-Home-Directory

            currUser = users.GetUserByGuid(req.UserGuid);
            if (currUser == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath unable to retrieve user object from GUID " + req.UserGuid);
                return null;
            }

            if (String.IsNullOrEmpty(currUser.HomeDirectory))
            {
                // global directory
                homeDirectory = String.Copy(settings.Storage.Directory);
                if (!homeDirectory.EndsWith(Common.GetPathSeparator(settings.Environment))) homeDirectory += Common.GetPathSeparator(settings.Environment);
                homeDirectory += currUser.Guid;
                homeDirectory += Common.GetPathSeparator(settings.Environment);
            }
            else
            {
                // user-specific home directory
                homeDirectory = String.Copy(currUser.HomeDirectory);
                if (!homeDirectory.EndsWith(Common.GetPathSeparator(settings.Environment))) homeDirectory += Common.GetPathSeparator(settings.Environment);
            }

            #endregion

            #region Process

            fullPath = String.Copy(homeDirectory);

            if (req.ContainerPath != null)
            {
                if (req.ContainerPath.Count > 0)
                {
                    foreach (string currContainer in req.ContainerPath)
                    {
                        if (String.IsNullOrEmpty(currContainer)) continue;

                        if (Common.ContainsUnsafeCharacters(currContainer))
                        {
                            logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath unsafe characters detected: " + currContainer);
                            return null;
                        }

                        fullPath += currContainer + Common.GetPathSeparator(settings.Environment);
                    }
                }
            }

            if (!String.IsNullOrEmpty(req.Key)) fullPath += req.Key; 
            return fullPath;

            #endregion
        }

        #endregion
    }
}
