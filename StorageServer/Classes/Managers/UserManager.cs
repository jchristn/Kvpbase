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
        #region Public-Members

        #endregion

        #region Private-Members

        private Events _Logging;
        private List<UserMaster> _Users;
        private readonly object _UsersLock;

        #endregion

        #region Constructors-and-Factories

        public UserManager(Events logging)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            _Logging = logging;
            _Users = new List<UserMaster>();
            _UsersLock = new object();
        }

        public UserManager(Events logging, List<UserMaster> curr)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            _Logging = logging;
            _Users = new List<UserMaster>();
            _UsersLock = new object();
            if (curr != null && curr.Count > 0)
            {
                _Users = new List<UserMaster>(curr);
            }
        }

        #endregion

        #region Public-Methods

        public void Add(UserMaster curr)
        {
            if (curr == null) return;
            lock (_UsersLock)
            {
                _Users.Add(curr);
            }
            return;
        }

        public void Remove(UserMaster curr)
        {
            if (curr == null) return;
            lock (_UsersLock)
            {
                if (_Users.Contains(curr)) _Users.Remove(curr);
            }
            return;
        }

        public List<UserMaster> GetUsers()
        {
            List<UserMaster> curr = new List<UserMaster>();
            lock (_UsersLock)
            {
                curr = new List<UserMaster>(_Users);
            }
            return curr;
        }

        public UserMaster GetUserByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) return null;
            lock (_UsersLock)
            {
                foreach (UserMaster curr in _Users)
                {
                    if (String.Compare(curr.Guid, guid) == 0) return curr;
                }
            }
            return null;
        }

        public UserMaster GetUserByEmail(string email)
        {
            if (String.IsNullOrEmpty(email)) return null;
            lock (_UsersLock)
            {
                foreach (UserMaster curr in _Users)
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
            lock (_UsersLock)
            {
                foreach (UserMaster curr in _Users)
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
            lock (_UsersLock)
            {
                foreach (UserMaster curr in _Users)
                {
                    if (String.Compare(curr.HomeDirectory, directory) == 0)
                    {
                        ret.Add(curr);
                    }
                }
            }
            return ret;
        }

        public bool AuthenticateCredentials(string email, string password, out UserMaster curr)
        {
            curr = null;
            if (String.IsNullOrEmpty(email)) return false;
            if (String.IsNullOrEmpty(password)) return false;

            curr = GetUserByEmail(email);
            if (curr == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AuthenticateCredentials unable to find email " + email);
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
                        _Logging.Log(LoggingModule.Severity.Warn, "AuthenticateCredentials UserMasterId " + curr.UserMasterId + " expired at " + curr.Expiration);
                        return false;
                    }
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "AuthenticateCredentials UserMasterId " + curr.UserMasterId + " marked inactive");
                    return false;
                }
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AuthenticateCredentials invalid password supplied for email " + email + " (" + password + " vs " + curr.Password + ")");
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

        public string GetHomeDirectory(string guid, Settings settings)
        {
            if (String.IsNullOrEmpty(guid)) return null;
            if (settings == null) return null;

            UserMaster curr = GetUserByGuid(guid);
            if (curr == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "GetHomeDirectory unable to find user GUID " + guid);
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

        #region Private-Methods

        #endregion
    }
}
