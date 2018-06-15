using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public class ApiKeyManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private UserManager _Users;

        private readonly object _ApiKeyLock;
        private List<ApiKey> _ApiKeys;

        private readonly object _ApiKeyPermissionsLock;
        private List<ApiKeyPermission> _ApiKeyPermissions;
        
        #endregion

        #region Constructors-and-Factories

        public ApiKeyManager(Settings settings, LoggingModule logging, UserManager users)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (users == null) throw new ArgumentNullException(nameof(users));

            _Settings = settings;
            _Logging = logging;
            _Users = users;

            _ApiKeyLock = new object();
            _ApiKeys = new List<ApiKey>();

            _ApiKeyPermissionsLock = new object();
            _ApiKeyPermissions = new List<ApiKeyPermission>();

            LoadApiKeysFile();
            LoadApiKeyPermissionsFile();
        }
         
        #endregion

        #region Public-Methods

        public void Add(ApiKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            lock (_ApiKeyLock)
            {
                _ApiKeys.Add(key);
            }

            SaveApiKeysFile();
            return;
        }

        public void Remove(ApiKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            lock (_ApiKeyLock)
            {
                if (_ApiKeys.Contains(key)) _ApiKeys.Remove(key);
            }

            SaveApiKeysFile();
            return;
        }

        public List<ApiKey> GetApiKeys()
        {
            List<ApiKey> curr = new List<ApiKey>();
            lock (_ApiKeyLock)
            {
                curr = new List<ApiKey>(_ApiKeys);
            }
            return curr;
        }

        public ApiKey GetApiKeyByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) return null;
            lock (_ApiKeyLock)
            {
                foreach (ApiKey curr in _ApiKeys)
                {
                    if (String.Compare(curr.Guid, guid) == 0) return curr;
                }
            }
            return null;
        }

        public ApiKey GetApiKeyById(int? id)
        {
            if (id == null) return null;
            int idInternal = Convert.ToInt32(id);
            lock (_ApiKeyLock)
            {
                foreach (ApiKey curr in _ApiKeys)
                {
                    if (curr.ApiKeyId == idInternal) return curr;
                }
            }
            return null;
        }

        public List<ApiKeyPermission> GetPermissionsByApiKeyId(int? apiKeyId)
        {
            List<ApiKeyPermission> ret = new List<ApiKeyPermission>();
            lock (_ApiKeyPermissionsLock)
            {
                foreach (ApiKeyPermission curr in _ApiKeyPermissions)
                {
                    if (curr.ApiKeyId == apiKeyId)
                    {
                        ret.Add(curr);
                    }
                }
            }
            return ret;
        }

        public ApiKeyPermission GetEffectivePermissions(int? apiKeyId, int? userMasterId)
        {
            ApiKeyPermission ret = new ApiKeyPermission();
            ret.ApiKeyPermissionId = 0;
            ret.ApiKeyId = 0;
            ret.UserMasterId = Convert.ToInt32(userMasterId);
            ret.ReadObject = false;
            ret.ReadContainer = false;
            ret.WriteObject = false;
            ret.WriteContainer = false;
            ret.DeleteObject = false;
            ret.DeleteContainer = false; 
            ret.Active = true;

            if (userMasterId != null) ret.UserMasterId = Convert.ToInt32(userMasterId);

            if (apiKeyId == null)
            {
                ret.ApiKeyId = 0;
                ret.ReadObject = true;
                ret.ReadContainer = true;
                ret.WriteObject = true;
                ret.WriteContainer = true;
                ret.DeleteObject = true;
                ret.DeleteContainer = true; 
                ret.Active = true;
                return ret;
            }
            else
            {
                ret.ApiKeyId = Convert.ToInt32(apiKeyId);

                List<ApiKeyPermission> perms = GetPermissionsByApiKeyId(apiKeyId);

                if (perms != null && perms.Count > 0)
                {
                    foreach (ApiKeyPermission curr in perms)
                    {
                        if (!curr.Active) continue;
                        if (!Common.IsLaterThanNow(curr.Expiration)) continue;
                        ret.ApiKeyPermissionId = curr.ApiKeyPermissionId;

                        if (curr.ReadObject) ret.ReadObject = true;
                        if (curr.ReadContainer) ret.ReadContainer = true;
                        if (curr.WriteObject) ret.WriteObject = true;
                        if (curr.WriteContainer) ret.WriteContainer = true;
                        if (curr.DeleteObject) ret.DeleteObject = true;
                        if (curr.DeleteContainer) ret.DeleteContainer = true; 
                    }
                }

                return ret;
            }
        }

        public bool VerifyApiKey(
            string apiKey, 
            out UserMaster currUserMaster,
            out ApiKey currApiKey,
            out ApiKeyPermission currPermission
            )
        {
            currUserMaster = new UserMaster();
            currApiKey = new ApiKey();
            currPermission = new ApiKeyPermission();

            currApiKey = GetApiKeyByGuid(apiKey);
            if (currApiKey == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "VerifyApiKey unable to retrieve API key " + apiKey);
                return false;
            }

            currPermission = GetEffectivePermissions(currApiKey.ApiKeyId, currUserMaster.UserMasterId);
            if (currPermission == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "VerifyApiKey unable to build ApiKeyPermission object for UserMasterId " + currUserMaster.UserMasterId);
                return false;
            } 

            if (currApiKey.Active)
            {
                #region Check-Key-Expiration

                if (Common.IsLaterThanNow(currApiKey.Expiration))
                {
                    currUserMaster = _Users.GetUserById(currApiKey.UserMasterId);
                    if (currUserMaster == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "VerifyApiKey unable to find UserMasterId " + currApiKey.UserMasterId);
                        return false;
                    }

                    if (currUserMaster.Active == 1)
                    {
                        #region Check-User-Expiration

                        if (Common.IsLaterThanNow(currUserMaster.Expiration))
                        {
                            currPermission.UserMasterId = currUserMaster.UserMasterId;
                            return true;
                        }
                        else
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "VerifyApiKey UserMasterId " + currUserMaster.UserMasterId + " expired at " + currUserMaster.Expiration);
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "VerifyApiKey UserMasterId " + currUserMaster.UserMasterId + " marked inactive");
                        return false;
                    }
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "VerifyApiKey ApiKeyId " + currApiKey.ApiKeyId + " expired at " + currApiKey.Expiration);
                    return false;
                }

                #endregion
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "VerifyApiKey ApiKeyId " + currApiKey.ApiKeyId + " marked inactive");
                return false;
            }
        }

        #endregion

        #region Private-Methods

        private void LoadApiKeysFile()
        {
            lock (_ApiKeyLock)
            {
                _ApiKeys = Common.DeserializeJson<List<ApiKey>>(Common.ReadBinaryFile(_Settings.Files.ApiKey));
            }
        }

        private void SaveApiKeysFile()
        {
            lock (_ApiKeyLock)
            {
                Common.WriteFile(
                    _Settings.Files.UserMaster,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(_ApiKeys, true)));
            }
        }

        private void LoadApiKeyPermissionsFile()
        {
            lock (_ApiKeyPermissionsLock)
            {
                _ApiKeyPermissions = Common.DeserializeJson<List<ApiKeyPermission>>(Common.ReadBinaryFile(_Settings.Files.Permission));
            }
        }

        private void SaveApiKeyPermissionsFile()
        {
            lock (_ApiKeyPermissionsLock)
            {
                Common.WriteFile(
                    _Settings.Files.Permission,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(_ApiKeyPermissions, true)));
            }
        }

        #endregion
    }
}
