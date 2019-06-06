using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Core;

namespace Kvpbase.Classes.Managers
{
    public class UserManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private readonly object _UsersLock;
        private List<UserMaster> _Users;
        
        #endregion

        #region Constructors-and-Factories

        public UserManager(Settings settings, LoggingModule logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            _Settings = settings;
            _Logging = logging;
            _Users = new List<UserMaster>();
            _UsersLock = new object();

            LoadUsersFile();
        }
         
        #endregion

        #region Public-Methods

        public void Add(UserMaster user)
        {
            if (user == null) throw new ArgumentNullException(nameof(user));

            lock (_UsersLock)
            {
                _Users.Add(user);
            }

            SaveUsersFile();
            return;
        }

        public void Remove(UserMaster user)
        {
            if (user == null) throw new ArgumentNullException(nameof(user));

            lock (_UsersLock)
            {
                if (_Users.Contains(user)) _Users.Remove(user);
            }

            SaveUsersFile();
            return;
        }

        public List<UserMaster> GetUsers()
        {
            List<UserMaster> users = new List<UserMaster>();

            lock (_UsersLock)
            {
                users = new List<UserMaster>(_Users);
            }

            return users;
        }

        public UserMaster GetUserByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            lock (_UsersLock)
            {
                UserMaster user = _Users.Where(u => u.Guid.Equals(guid)).FirstOrDefault();
                if (user == default(UserMaster)) user = null;
                return user;
            }
        }

        public UserMaster GetUserByEmail(string email)
        {
            if (String.IsNullOrEmpty(email)) return null;
            lock (_UsersLock)
            {
                UserMaster user = _Users.Where(u => u.Email.Equals(email)).FirstOrDefault();
                if (user == default(UserMaster)) user = null;
                return user;
            }
        }

        public UserMaster GetUserById(int? id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));

            lock (_UsersLock)
            {
                UserMaster user = _Users.Where(u => u.UserMasterId.Equals(id)).FirstOrDefault();
                if (user == default(UserMaster)) user = null;
                return user;
            }
        }

        public List<UserMaster> GetUsersByHomeDirectory(string directory)
        {
            if (String.IsNullOrEmpty(directory)) throw new ArgumentNullException(nameof(directory));

            List<UserMaster> ret = new List<UserMaster>();
            lock (_UsersLock)
            {
                ret = _Users.Where(u => u.HomeDirectory.Equals(directory)).ToList();
                return ret;
            }
        }

        public bool Authenticate(string email, string password, out UserMaster curr)
        {
            curr = null;
            if (String.IsNullOrEmpty(email)) throw new ArgumentNullException(nameof(email));
            if (String.IsNullOrEmpty(password)) throw new ArgumentNullException(nameof(password));

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
         
        public string GetHomeDirectory(string guid)
        {
            if (String.IsNullOrEmpty(guid)) return null; 

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
                string ret = String.Copy(_Settings.Storage.Directory);

                if (!ret.EndsWith("/")) ret += "/";
                ret += curr.Guid + "/";
                return ret;
            }
        }

        #endregion

        #region Private-Methods

        private void LoadUsersFile()
        {
            lock (_UsersLock)
            {
                _Users = Common.DeserializeJson<List<UserMaster>>(Common.ReadBinaryFile(_Settings.Files.UserMaster));
            }
        }

        private void SaveUsersFile()
        {
            lock (_UsersLock)
            {
                Common.WriteFile(
                    _Settings.Files.UserMaster,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(_Users, true)));
            }
        }

        #endregion
    }
}
