using System;
using System.Collections.Generic;
using SyslogLogging;

namespace Kvpbase
{
    public class MoveRequest
    {
        #region Public-Members

        public string UserGuid;
        public List<string> FromContainer;
        public string MoveFrom;
        public List<string> ToContainer;
        public string MoveTo;

        #endregion

        #region Constructors-and-Factories

        public MoveRequest()
        {

        }

        #endregion

        #region Public-Static-Methods

        public static string BuildDiskPath(MoveRequest req, bool useMoveFrom, bool includeObjectName, UserManager users, Settings settings, Events logging)
        {
            #region Check-for-Null-Values

            if (req == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath null move object supplied");
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

            if (useMoveFrom)
            {
                foreach (string currContainer in req.FromContainer)
                {
                    if (String.IsNullOrEmpty(currContainer)) continue;

                    if (Common.ContainsUnsafeCharacters(currContainer))
                    {
                        logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath unsafe characters detected: " + currContainer);
                        return null;
                    }

                    fullPath += currContainer + Common.GetPathSeparator(settings.Environment);
                }

                if (includeObjectName) if (!String.IsNullOrEmpty(req.MoveFrom)) fullPath += req.MoveFrom;
            }
            else
            {
                foreach (string currContainer in req.ToContainer)
                {
                    if (String.IsNullOrEmpty(currContainer)) continue;

                    if (Common.ContainsUnsafeCharacters(currContainer))
                    {
                        logging.Log(LoggingModule.Severity.Warn, "BuildDiskPath unsafe characters detected: " + currContainer);
                        return null;
                    }

                    fullPath += currContainer + Common.GetPathSeparator(settings.Environment);
                }

                if (includeObjectName) if (!String.IsNullOrEmpty(req.MoveFrom)) fullPath += req.MoveTo;
            }

            // EventHandler.Log(LoggingModule.Severity.Debug, "BuildDiskPath_MoveFrom returning full_path " + full_path);
            return fullPath;

            #endregion
        }

        public static bool UnsafeFsChars(MoveRequest currMove)
        {
            if (currMove == null) return true;
            if (Common.ContainsUnsafeCharacters(currMove.FromContainer)) return true;
            if (Common.ContainsUnsafeCharacters(currMove.ToContainer)) return true;
            if (Common.ContainsUnsafeCharacters(currMove.MoveFrom)) return true;
            if (Common.ContainsUnsafeCharacters(currMove.MoveTo)) return true;
            return false;
        }

        #endregion
    }
}
