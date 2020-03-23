using System;
using System.Collections.Generic;
using System.Text;
using DatabaseWrapper;
using SyslogLogging;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer.Classes.Managers
{
    internal class AuthManager
    {
        private Settings _Settings;
        private LoggingModule _Logging;
        private DatabaseManager _Database;
        // private string _Header = "[Kvpbase.AuthManager] ";

        internal AuthManager(Settings settings, LoggingModule logging, DatabaseManager database)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (database == null) throw new ArgumentNullException(nameof(database));

            _Settings = settings;
            _Logging = logging;
            _Database = database;
        }
         
        #region API-Key-Methods

        internal void AddApiKey(ApiKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key)); 
            if (ApiKeyExists(key.GUID)) return;
            _Database.Insert<ApiKey>(key);
        }

        internal void RemoveApiKey(ApiKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            _Database.Delete<ApiKey>(key);

            List<Permission> perms = GetPermissionsByApiKey(key.GUID);
            if (perms != null && perms.Count > 0)
            {
                foreach (Permission curr in perms)
                    _Database.Delete<Permission>(curr);
            }
        }

        internal bool ApiKeyExists(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            ApiKey key = _Database.SelectByGUID<ApiKey>(guid);
            if (key != null) return true;
            return false;
        }

        internal List<ApiKey> GetApiKeys()
        {
            return _Database.SelectMany<ApiKey>(null, null, null, "ORDER BY id DESC");
        }

        internal ApiKey GetApiKeyByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            return _Database.SelectByGUID<ApiKey>(guid);
        }

        internal List<ApiKey> GetApiKeysByUserGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            Expression e = new Expression("userguid", Operators.Equals, guid);
            return _Database.SelectMany<ApiKey>(null, null, e, "ORDER BY id DESC");
        }

        #endregion

        #region Permission-Methods

        internal void AddPermission(Permission perm)
        {
            if (perm == null) throw new ArgumentNullException(nameof(perm));
            if (PermissionsExist(perm.ContainerGUID)) return;
            _Database.Insert<Permission>(perm);
        }

        internal void RemovePermission(Permission perm)
        {
            if (perm == null) throw new ArgumentNullException(nameof(perm));
            _Database.Delete<Permission>(perm);
        }

        internal bool PermissionsExist(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            Permission perm = _Database.SelectByGUID<Permission>(guid);
            if (perm != null) return true;
            return false;
        }

        internal List<Permission> GetPermissions()
        {
            return _Database.SelectMany<Permission>(null, null, null, "ORDER BY id DESC");
        }

        internal Permission GetPermissionByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            return _Database.SelectByGUID<Permission>(guid);
        }

        internal List<Permission> GetPermissionsByApiKey(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid)); 
            List<Permission> ret = new List<Permission>(); 
            ApiKey key = GetApiKeyByGuid(guid);
            if (key == null) return ret;
            Expression e = new Expression("apikeyguid", Operators.Equals, guid);
            return _Database.SelectMany<Permission>(null, null, e, "ORDER BY id DESC");
        }

        internal List<Permission> GetPermissionsByApiKeyId(int id)
        { 
            List<Permission> ret = new List<Permission>();
            ApiKey key = _Database.SelectById<ApiKey>(id);
            if (key == null) return ret;
            Expression e = new Expression("apikeyguid", Operators.Equals, key.GUID);
            return _Database.SelectMany<Permission>(null, null, e, "ORDER BY id DESC");
        }

        internal List<Permission> GetPermissionsByUserGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid)); 
            List<Permission> ret = new List<Permission>(); 
            UserMaster user = GetUserByGuid(guid);
            if (user == null) return ret; 
            Expression e = new Expression("userguid", Operators.Equals, user.GUID);
            return _Database.SelectMany<Permission>(null, null, e, "ORDER BY id DESC");
        }

        internal List<Permission> GetPermissionsByUserId(int id)
        { 
            List<Permission> ret = new List<Permission>();
            UserMaster user = GetUserById(id);
            if (user == null) return ret;
            Expression e = new Expression("userguid", Operators.Equals, user.GUID);
            return _Database.SelectMany<Permission>(null, null, e, "ORDER BY id DESC");
        }

        #endregion

        #region User-Methods

        internal void AddUser(UserMaster user)
        {
            if (user == null) throw new ArgumentNullException(nameof(user)); 
            if (UserExistsByGuid(user.GUID)) return;
            if (UserExistsByEmail(user.Email)) return;
            _Database.Insert<UserMaster>(user);
        }

        internal void RemoveUser(UserMaster user)
        {
            if (user == null) throw new ArgumentNullException(nameof(user));
            _Database.Delete<UserMaster>(user);

            List<ApiKey> keys = GetApiKeysByUserGuid(user.GUID);
            if (keys != null && keys.Count > 0)
            {
                foreach (ApiKey curr in keys)
                    RemoveApiKey(curr);
            }
        }

        internal bool UserExistsByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            UserMaster user = _Database.SelectByGUID<UserMaster>(guid);
            if (user != null) return true;
            return false;
        }

        internal bool UserExistsByEmail(string email)
        {
            if (String.IsNullOrEmpty(email)) throw new ArgumentNullException(nameof(email));
            Expression e = new Expression("email", Operators.Equals, email);
            UserMaster user = _Database.SelectByFilter<UserMaster>(e, "ORDER BY id DESC");
            if (user != null) return true;
            return false;
        }

        internal List<UserMaster> GetUsers()
        {
            return _Database.SelectMany<UserMaster>(null, null, null, "ORDER BY id DESC");
        }

        internal UserMaster GetUserById(int id)
        {
            return _Database.SelectById<UserMaster>(id);
        }

        internal UserMaster GetUserByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            return _Database.SelectByGUID<UserMaster>(guid);
        }

        internal UserMaster GetUserByEmail(string email)
        {
            if (String.IsNullOrEmpty(email)) throw new ArgumentNullException(nameof(email));
            Expression e = new Expression("email", Operators.Equals, email);
            return _Database.SelectByFilter<UserMaster>(e, "ORDER BY id DESC");
        }

        #endregion

        #region Authentication-Methods

        internal Permission GetEffectivePermissions(string apiKeyGuid)
        {
            if (String.IsNullOrEmpty(apiKeyGuid)) throw new ArgumentNullException(nameof(apiKeyGuid));

            ApiKey key = GetApiKeyByGuid(apiKeyGuid);

            Permission ret = new Permission();
            ret.Id = 0;
            ret.ContainerGUID = Guid.NewGuid().ToString();
            ret.UserGUID = key.UserGUID;
            ret.ApiKeyGUID = apiKeyGuid;
            ret.Notes = "*** System-generated effective permissions ***";
            ret.ReadObject = false;
            ret.ReadContainer = false;
            ret.WriteObject = false;
            ret.WriteContainer = false;
            ret.DeleteObject = false;
            ret.DeleteContainer = false;
            ret.Active = false;

            if (!String.IsNullOrEmpty(apiKeyGuid))
            {
                List<Permission> apiKeyPermissions = GetPermissionsByApiKey(apiKeyGuid);
                if (apiKeyPermissions != null && apiKeyPermissions.Count > 0)
                {
                    foreach (Permission perm in apiKeyPermissions)
                    {
                        ret.ReadObject |= perm.ReadObject;
                        ret.ReadContainer |= perm.ReadContainer;
                        ret.WriteObject |= perm.WriteObject;
                        ret.WriteContainer |= perm.WriteContainer;
                        ret.DeleteObject |= perm.DeleteObject;
                        ret.DeleteContainer |= perm.DeleteContainer;
                    }
                }
            } 

            return ret;
        }

        internal bool Authenticate(
            string apiKeyGuid,
            out UserMaster user,
            out ApiKey apiKey,
            out Permission effectivePermissions,
            out AuthResult result
            )
        {
            user = null;
            apiKey = null;
            effectivePermissions = null;
            result = AuthResult.NoMaterialSupplied;

            if (String.IsNullOrEmpty(apiKeyGuid)) return false;

            apiKey = GetApiKeyByGuid(apiKeyGuid);
            if (apiKey == null)
            {
                result = AuthResult.ApiKeyNotFound;
                return false;
            }

            if (!apiKey.Active)
            {
                result = AuthResult.ApiKeyInactive;
                return false;
            }

            user = GetUserByGuid(apiKey.UserGUID);
            if (user == null)
            {
                result = AuthResult.UserNotFound;
                return false;
            }

            if (!user.Active)
            {
                result = AuthResult.UserInactive;
                return false;
            }

            effectivePermissions = GetEffectivePermissions(apiKey.GUID);
            result = AuthResult.Success;
            return true;
        }

        #endregion
    }
}
