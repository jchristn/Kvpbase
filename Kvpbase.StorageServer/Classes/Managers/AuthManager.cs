using System;
using System.Collections.Generic;
using System.Text;
using DatabaseWrapper;
using SyslogLogging;
using Watson.ORM;
using Watson.ORM.Core;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer.Classes.Managers
{
    internal class AuthManager
    {
        private Settings _Settings;
        private LoggingModule _Logging;
        private WatsonORM _ORM;
        // private string _Header = "[Kvpbase.AuthManager] ";

        internal AuthManager(Settings settings, LoggingModule logging, WatsonORM orm)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (orm == null) throw new ArgumentNullException(nameof(orm));

            _Settings = settings;
            _Logging = logging;
            _ORM = orm;
        }
         
        #region API-Key-Methods

        internal void AddApiKey(ApiKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key)); 
            if (ApiKeyExists(key.GUID)) return;
            _ORM.Insert<ApiKey>(key);
        }

        internal void RemoveApiKey(ApiKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            _ORM.Delete<ApiKey>(key);

            List<Permission> perms = GetPermissionsByApiKey(key.GUID);
            if (perms != null && perms.Count > 0)
            {
                foreach (Permission curr in perms)
                    _ORM.Delete<Permission>(curr);
            }
        }

        internal bool ApiKeyExists(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<ApiKey>(nameof(ApiKey.GUID)),
                DbOperators.Equals,
                guid);

            ApiKey key = _ORM.SelectFirst<ApiKey>(e);
            if (key != null) return true;
            return false;
        }

        internal List<ApiKey> GetApiKeys()
        {
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<ApiKey>(nameof(ApiKey.Id)),
                DbOperators.GreaterThan,
                0);

            return _ORM.SelectMany<ApiKey>(e);
        }

        internal ApiKey GetApiKeyByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<ApiKey>(nameof(ApiKey.GUID)),
                DbOperators.Equals,
                guid);

            return _ORM.SelectFirst<ApiKey>(e);
        }

        internal List<ApiKey> GetApiKeysByUserGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<ApiKey>(nameof(ApiKey.UserGUID)),
                DbOperators.Equals,
                guid);

            return _ORM.SelectMany<ApiKey>(e);
        }

        #endregion

        #region Permission-Methods

        internal void AddPermission(Permission perm)
        {
            if (perm == null) throw new ArgumentNullException(nameof(perm));
            if (PermissionsExist(perm.ContainerGUID)) return;
            _ORM.Insert<Permission>(perm);
        }

        internal void RemovePermission(Permission perm)
        {
            if (perm == null) throw new ArgumentNullException(nameof(perm));
            _ORM.Delete<Permission>(perm);
        }

        internal bool PermissionsExist(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<Permission>(nameof(Permission.GUID)),
                DbOperators.Equals,
                guid);

            Permission perm = _ORM.SelectFirst<Permission>(e);
            if (perm != null) return true;
            return false;
        }

        internal List<Permission> GetPermissions()
        {
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<Permission>(nameof(Permission.Id)),
                DbOperators.GreaterThan,
                0);

            return _ORM.SelectMany<Permission>(e);
        }

        internal Permission GetPermissionByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<Permission>(nameof(Permission.GUID)),
                DbOperators.Equals,
                guid);

            return _ORM.SelectFirst<Permission>(e);
        }

        internal List<Permission> GetPermissionsByApiKey(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));  

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<Permission>(nameof(Permission.ApiKeyGUID)),
                DbOperators.Equals,
                guid);
             
            return _ORM.SelectMany<Permission>(e);
        }

        internal List<Permission> GetPermissionsByApiKeyId(int id)
        {
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<ApiKey>(nameof(ApiKey.Id)),
                DbOperators.Equals,
                id);

            ApiKey key = _ORM.SelectFirst<ApiKey>(e);
            if (key == null) return new List<Permission>();
            return GetPermissionsByApiKey(key.GUID);
        }

        internal List<Permission> GetPermissionsByUserGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<Permission>(nameof(Permission.UserGUID)),
                DbOperators.Equals,
                guid);

            return _ORM.SelectMany<Permission>(e);
        }

        internal List<Permission> GetPermissionsByUserId(int id)
        {
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UserMaster>(nameof(UserMaster.Id)),
                DbOperators.Equals,
                id);

            UserMaster user = _ORM.SelectFirst<UserMaster>(e);
            if (user == null) return new List<Permission>();
            return GetPermissionsByUserGuid(user.GUID);
        }

        #endregion

        #region User-Methods

        internal void AddUser(UserMaster user)
        {
            if (user == null) throw new ArgumentNullException(nameof(user)); 
            if (UserExistsByGuid(user.GUID)) return;
            if (UserExistsByEmail(user.Email)) return;
            _ORM.Insert<UserMaster>(user);
        }

        internal void RemoveUser(UserMaster user)
        {
            if (user == null) throw new ArgumentNullException(nameof(user));
            _ORM.Delete<UserMaster>(user);

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

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UserMaster>(nameof(UserMaster.GUID)),
                DbOperators.Equals,
                guid);

            UserMaster user = _ORM.SelectFirst<UserMaster>(e);
            if (user != null) return true;
            return false;
        }

        internal bool UserExistsByEmail(string email)
        {
            if (String.IsNullOrEmpty(email)) throw new ArgumentNullException(nameof(email));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UserMaster>(nameof(UserMaster.Email)),
                DbOperators.Equals,
                email);

            UserMaster user = _ORM.SelectFirst<UserMaster>(e);
            if (user != null) return true;
            return false;
        }

        internal List<UserMaster> GetUsers()
        {
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UserMaster>(nameof(UserMaster.Id)),
                DbOperators.GreaterThan,
                0);

            return _ORM.SelectMany<UserMaster>(e);
        }

        internal UserMaster GetUserById(int id)
        {
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UserMaster>(nameof(UserMaster.Id)),
                DbOperators.Equals,
                id);

            return _ORM.SelectFirst<UserMaster>(e);
        }

        internal UserMaster GetUserByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UserMaster>(nameof(UserMaster.GUID)),
                DbOperators.Equals,
                guid);

            return _ORM.SelectFirst<UserMaster>(e);
        }

        internal UserMaster GetUserByEmail(string email)
        {
            if (String.IsNullOrEmpty(email)) throw new ArgumentNullException(nameof(email));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UserMaster>(nameof(UserMaster.Email)),
                DbOperators.Equals,
                email);

            return _ORM.SelectFirst<UserMaster>(e);
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
                        ret.IsAdmin |= perm.IsAdmin;
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
