using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public class UserManager
    {
        #region Private-Members

        private List<UserMaster> Users;
        private readonly object UserLock;

        #endregion

        #region Constructors-and-Factories

        public UserManager()
        {
            Users = new List<UserMaster>();
            UserLock = new object();
        }

        public UserManager(List<UserMaster> curr)
        {
            Users = new List<UserMaster>();
            UserLock = new object();
            if (curr != null && curr.Count > 0)
            {
                Users = new List<UserMaster>(curr);
            }
        }

        #endregion

        #region Public-Methods

        public void Add(UserMaster curr)
        {
            if (curr == null) return;
            lock (UserLock)
            {
                Users.Add(curr);
            }
            return;
        }
        
        public void Remove(UserMaster curr)
        {
            if (curr == null) return;
            lock (UserLock)
            {
                if (Users.Contains(curr)) Users.Remove(curr);
            }
            return;
        }

        public List<UserMaster> GetUsers()
        {
            List<UserMaster> curr = new List<UserMaster>();
            lock (UserLock)
            {
                curr = new List<UserMaster>(Users);
            }
            return curr;
        }

        public UserMaster GetUserByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) return null;
            lock (UserLock)
            {
                foreach (UserMaster curr in Users)
                {
                    if (String.Compare(curr.Guid, guid) == 0) return curr;
                }
            }
            return null;
        }

        public UserMaster GetUserByEmail(string email)
        {
            if (String.IsNullOrEmpty(email)) return null;
            lock (UserLock)
            {
                foreach (UserMaster curr in Users)
                {
                    if (String.Compare(curr.Email, email) == 0) return curr;
                }
            }
            return null;
        }

        public UserMaster GetUserById(int? id)
        {
            if (id == null) return null;
            int idInternal = Convert.ToInt32(id);
            lock (UserLock)
            {
                foreach (UserMaster curr in Users)
                {
                    if (curr.UserMasterId == idInternal) return curr;
                }
            }
            return null;
        }

        public List<UserMaster> GetUsersByHomeDirectory(string directory)
        {
            if (String.IsNullOrEmpty(directory)) return null;
            List<UserMaster> ret = new List<UserMaster>();
            lock (UserLock)
            {
                foreach (UserMaster curr in Users)
                {
                    if (String.Compare(curr.HomeDirectory, directory) == 0)
                    {
                        ret.Add(curr);
                    }
                }
            }
            return ret;
        }

        public bool AuthenticateCredentials(string email, string password, Events logging, out UserMaster curr)
        {
            curr = null;
            if (String.IsNullOrEmpty(email)) return false;
            if (String.IsNullOrEmpty(password)) return false;

            curr = GetUserByEmail(email);
            if (curr == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "AuthenticateCredentials unable to find email " + email);
                return false;
            }
            
            if (String.Compare(curr.Password, password) == 0)
            {
                if (curr.Active == 1)
                {
                    if (Common.IsLaterThanNow(curr.Expiration))
                    {
                        return true;
                    }
                    else
                    {
                        logging.Log(LoggingModule.Severity.Warn, "AuthenticateCredentials UserMasterId " + curr.UserMasterId + " expired at " + curr.Expiration);
                        return false;
                    }
                }
                else
                {
                    logging.Log(LoggingModule.Severity.Warn, "AuthenticateCredentials UserMasterId " + curr.UserMasterId + " marked inactive");
                    return false;
                }
            }
            else
            {
                logging.Log(LoggingModule.Severity.Warn, "AuthenticateCredentials invalid password supplied for email " + email + " (" + password + " vs " + curr.Password + ")");
                return false;
            }
        }

        public bool GetGatewayMode(string guid, Settings settings)
        {
            if (String.IsNullOrEmpty(guid)) return false;
            if (settings == null) return false;
            UserMaster curr = GetUserByGuid(guid);
            return curr.GetGatewayMode(settings);
        }

        public string GetHomeDirectory(string guid, Settings settings, Events logging)
        {
            if (String.IsNullOrEmpty(guid)) return null;
            if (settings == null) return null;

            UserMaster curr = GetUserByGuid(guid);
            if (curr == null)
            {
                logging.Log(LoggingModule.Severity.Warn, "GetHomeDirectory unable to find user GUID " + guid);
                return null;
            }

            if (!String.IsNullOrEmpty(curr.HomeDirectory))
            {
                return curr.HomeDirectory;
            }
            else
            {
                string ret = String.Copy(settings.Storage.Directory);

                if (!ret.EndsWith(Common.GetPathSeparator(settings.Environment)))
                {
                    ret += Common.GetPathSeparator(settings.Environment);
                }

                ret += curr.Guid + Common.GetPathSeparator(settings.Environment);
                return ret;
            }
        }

        #endregion
    }
}
