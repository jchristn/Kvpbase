using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

using Kvpbase.Containers;

using DatabaseWrapper;
using SyslogLogging;
 
namespace Kvpbase.Classes.Managers
{
    public class ConfigManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private DatabaseClient _Database;

        private const string USER_TABLE = "kvpbase_users";
        private const string API_KEY_TABLE = "kvpbase_api_keys";
        private const string PERMISSION_TABLE = "kvpbase_permissions"; 
        private const string LOCKS_TABLE = "kvpbase_url_locks";
        private const string CONTAINERS_TABLE = "kvpbase_containers";

        #endregion

        #region Constructors-and-Factories

        public ConfigManager(Settings settings, LoggingModule logging, DatabaseClient database)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (database == null) throw new ArgumentNullException(nameof(database));

            _Settings = settings;
            _Logging = logging;
            _Database = database;

            InitializeTables();
        }

        #endregion

        #region Public-Methods

        #region API-Key-Methods

        public void AddApiKey(ApiKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (ApiKeyExists(key.GUID)) return;

            Dictionary<string, object> insertVals = key.ToInsertDictionary();
            DataTable result = _Database.Insert(API_KEY_TABLE, insertVals);
        }

        public void RemoveApiKey(ApiKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            Expression e = new Expression("GUID", Operators.Equals, key.GUID);
            _Database.Delete(API_KEY_TABLE, e);

            e = new Expression("ApiKeyId", Operators.Equals, key.Id);
            _Database.Delete(PERMISSION_TABLE, e);
        }

        public bool ApiKeyExists(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            Expression e = new Expression("GUID", Operators.Equals, guid);
            DataTable result = _Database.Select(API_KEY_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0) return true;
            return false;
        }

        public List<ApiKey> GetApiKeys()
        {
            List<ApiKey> ret = new List<ApiKey>();

            DataTable result = _Database.Select(API_KEY_TABLE, null, null, null, null, null);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(ApiKey.FromDataRow(row));
                }
            }

            return ret;
        }

        public ApiKey GetApiKeyByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Expression e = new Expression("GUID", Operators.Equals, guid);
            DataTable result = _Database.Select(API_KEY_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                return ApiKey.FromDataRow(result.Rows[0]);
            }

            return null;
        }

        #endregion

        #region Permission-Methods

        public void AddPermission(Permission perm)
        {
            if (perm == null) throw new ArgumentNullException(nameof(perm));

            if (PermissionsExist(perm.GUID)) return;

            Dictionary<string, object> insertVals = perm.ToInsertDictionary();
            DataTable result = _Database.Insert(PERMISSION_TABLE, insertVals);
        }

        public void RemovePermission(Permission perm)
        {
            if (perm == null) throw new ArgumentNullException(nameof(perm));

            Expression e = new Expression("GUID", Operators.Equals, perm.GUID);
            _Database.Delete(PERMISSION_TABLE, e);
        }

        public bool PermissionsExist(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            Expression e = new Expression("GUID", Operators.Equals, guid);
            DataTable result = _Database.Select(PERMISSION_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0) return true;
            return false;
        }

        public List<Permission> GetPermissions()
        {
            List<Permission> ret = new List<Permission>();

            DataTable result = _Database.Select(PERMISSION_TABLE, null, null, null, null, null);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(Permission.FromDataRow(row));
                }
            }

            return ret;
        }

        public Permission GetPermissionByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Permission ret = new Permission();

            Expression e = new Expression("GUID", Operators.Equals, guid);
            DataTable result = _Database.Select(PERMISSION_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                return Permission.FromDataRow(result.Rows[0]);
            }

            return null;
        }

        public List<Permission> GetPermissionsByApiKey(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            List<Permission> ret = new List<Permission>();

            ApiKey key = GetApiKeyByGuid(guid);
            if (key == null) return ret;

            Expression e = new Expression("ApiKeyId", Operators.Equals, key.Id);
            DataTable result = _Database.Select(PERMISSION_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(Permission.FromDataRow(result.Rows[0]));
                }
            }

            return ret;
        }

        public List<Permission> GetPermissionsByApiKeyId(int? apiKeyId)
        {
            if (apiKeyId == null) throw new ArgumentNullException(nameof(apiKeyId));

            List<Permission> ret = new List<Permission>();

            Expression e = new Expression("ApiKeyId", Operators.Equals, apiKeyId);
            DataTable result = _Database.Select(PERMISSION_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(Permission.FromDataRow(result.Rows[0]));
                }
            }

            return ret;
        }

        public List<Permission> GetPermissionsByUserId(int? userId)
        {
            if (userId == null) throw new ArgumentNullException(nameof(userId));

            List<Permission> ret = new List<Permission>();

            Expression e = new Expression("UserMasterId", Operators.Equals, userId);
            DataTable result = _Database.Select(PERMISSION_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(Permission.FromDataRow(result.Rows[0]));
                }
            }

            return ret;
        }

        #endregion

        #region User-Methods

        public void AddUser(UserMaster user)
        {
            if (user == null) throw new ArgumentNullException(nameof(user));

            if (UserExists(user.GUID, user.Email)) return;

            Dictionary<string, object> insertVals = user.ToInsertDictionary();
            DataTable result = _Database.Insert(USER_TABLE, insertVals);
        }

        public void RemoveUser(UserMaster user)
        {
            if (user == null) throw new ArgumentNullException(nameof(user));

            Expression e = new Expression("GUID", Operators.Equals, user.GUID);
            _Database.Delete(USER_TABLE, e);

            e = new Expression("UserMasterId", Operators.Equals, user.Id);
            _Database.Delete(PERMISSION_TABLE, e);
            _Database.Delete(USER_TABLE, e);
        }

        public bool UserExists(string guid, string email)
        {
            if (String.IsNullOrEmpty(guid) && String.IsNullOrEmpty(email)) throw new ArgumentException("Either GUID or email must be supplied.");

            Expression e = null;
            DataTable result = null;

            if (!String.IsNullOrEmpty(guid))
            {
                e = new Expression("GUID", Operators.Equals, guid);
                result = _Database.Select(USER_TABLE, null, null, null, e, null);
                if (result != null && result.Rows.Count > 0) return true;
            }

            if (!String.IsNullOrEmpty(email))
            {
                e = new Expression("Email", Operators.Equals, email);
                result = _Database.Select(USER_TABLE, null, null, null, e, null);
                if (result != null && result.Rows.Count > 0) return true;
            }

            return false;
        }

        public List<UserMaster> GetUsers()
        {
            List<UserMaster> ret = new List<UserMaster>();

            DataTable result = _Database.Select(USER_TABLE, null, null, null, null, null);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(UserMaster.FromDataRow(row));
                }
            }

            return ret;
        }

        public UserMaster GetUserById(int? id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));

            Expression e = new Expression("Id", Operators.Equals, id);
            DataTable result = _Database.Select(USER_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                return UserMaster.FromDataRow(result.Rows[0]);
            }

            return null;
        }

        public UserMaster GetUserByGuid(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Expression e = new Expression("GUID", Operators.Equals, guid);
            DataTable result = _Database.Select(USER_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                return UserMaster.FromDataRow(result.Rows[0]);
            }

            return null;
        }

        public UserMaster GetUserByEmail(string email)
        {
            if (String.IsNullOrEmpty(email)) throw new ArgumentNullException(nameof(email));

            Expression e = new Expression("Email", Operators.Equals, email);
            DataTable result = _Database.Select(USER_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                return UserMaster.FromDataRow(result.Rows[0]);
            }

            return null;
        }

        #endregion

        #region Read-Lock-Methods
        
        /*
         *  
         * Read operations cannot continue if the URL is being written, but can continue if being read elsewhere.
         * 
         * 
         */

        public bool AddReadLock(RequestMetadata md)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));

            UrlLock urlLock = null;
            if (md.User != null && !String.IsNullOrEmpty(md.User.GUID))
            {
                urlLock = new UrlLock(LockType.Read, md.Http.Request.RawUrlWithoutQuery, md.User.GUID);
            }
            else
            {
                urlLock = new UrlLock(LockType.Read, md.Http.Request.RawUrlWithoutQuery, null);
            }

            return AddReadLock(urlLock);
        }

        public bool AddReadLock(string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));

            UrlLock urlLock = new UrlLock(LockType.Read, url, userGuid);
            return AddReadLock(urlLock);
        }

        public bool AddReadLock(UrlLock urlLock)
        {
            if (urlLock == null) throw new ArgumentNullException(nameof(urlLock));
            if (WriteLockExists(urlLock.Url)) return false;

            Dictionary<string, object> insertVals = urlLock.ToInsertDictionary();
            _Database.Insert(LOCKS_TABLE, insertVals);
            return true;
        }

        public void RemoveReadLock(RequestMetadata md)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));

            UrlLock urlLock = null;
            if (md.User != null && !String.IsNullOrEmpty(md.User.GUID))
            {
                urlLock = new UrlLock(LockType.Read, md.Http.Request.RawUrlWithoutQuery, md.User.GUID);
            }
            else
            {
                urlLock = new UrlLock(LockType.Read, md.Http.Request.RawUrlWithoutQuery, null);
            }

            RemoveReadLock(urlLock);
        }

        public void RemoveReadLock(UrlLock urlLock)
        {
            if (urlLock == null) throw new ArgumentNullException(nameof(urlLock));

            Expression e = new Expression("Url", Operators.Equals, urlLock.Url);
            e.PrependAnd(new Expression("LockType", Operators.Equals, LockType.Read.ToString()));

            if (!String.IsNullOrEmpty(urlLock.UserGuid)) e.PrependAnd(new Expression("UserGuid", Operators.Equals, urlLock.UserGuid));

            _Database.Delete(LOCKS_TABLE, e);
        }

        public void RemoveReadLock(string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));

            Expression e = new Expression("Url", Operators.Equals, url); 
            e.PrependAnd(new Expression("LockType", Operators.Equals, LockType.Read.ToString()));

            if (!String.IsNullOrEmpty(userGuid)) e.PrependAnd(new Expression("UserGuid", Operators.Equals, userGuid));

            _Database.Delete(LOCKS_TABLE, e);
        }

        public bool ReadLockExists(string url)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));

            Expression e = new Expression("Url", Operators.Equals, url);
            e.PrependAnd(new Expression("LockType", Operators.Equals, LockType.Read.ToString()));
            DataTable result = _Database.Select(LOCKS_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0) return true;
            return false;
        }

        public List<UrlLock> GetReadLocks()
        {
            List<UrlLock> ret = new List<UrlLock>();
            Expression e = new Expression("Id", Operators.GreaterThan, 0);
            e.PrependAnd(new Expression("LockType", Operators.Equals, LockType.Read.ToString()));
            DataTable result = _Database.Select(LOCKS_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(UrlLock.FromDataRow(row));
                }
            }
            return ret;
        }

        #endregion

        #region Write-Lock-Methods

        /*
         * 
         * Write operations cannot continue if the URL is being read or written elsewhere.
         * 
         * 
         */

        public bool AddWriteLock(RequestMetadata md)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));

            UrlLock urlLock = null;
            if (md.User != null && !String.IsNullOrEmpty(md.User.GUID))
            {
                urlLock = new UrlLock(LockType.Write, md.Http.Request.RawUrlWithoutQuery, md.User.GUID);
            }
            else
            {
                urlLock = new UrlLock(LockType.Write, md.Http.Request.RawUrlWithoutQuery, null);
            }

            return AddWriteLock(urlLock);
        }

        public bool AddWriteLock(string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));

            UrlLock urlLock = new UrlLock(LockType.Write, url, userGuid);
            return AddWriteLock(urlLock);
        }

        public bool AddWriteLock(UrlLock urlLock)
        {
            if (urlLock == null) throw new ArgumentNullException(nameof(urlLock));
            if (WriteLockExists(urlLock.Url)) return false;
            if (ReadLockExists(urlLock.Url)) return false;

            Dictionary<string, object> insertVals = urlLock.ToInsertDictionary();
            _Database.Insert(LOCKS_TABLE, insertVals);
            return true;
        }

        public void RemoveWriteLock(RequestMetadata md)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));

            UrlLock urlLock = null;
            if (md.User != null && !String.IsNullOrEmpty(md.User.GUID))
            {
                urlLock = new UrlLock(LockType.Write, md.Http.Request.RawUrlWithoutQuery, md.User.GUID);
            }
            else
            {
                urlLock = new UrlLock(LockType.Write, md.Http.Request.RawUrlWithoutQuery, null);
            }

            RemoveWriteLock(urlLock);
        }

        public void RemoveWriteLock(UrlLock urlLock)
        {
            if (urlLock == null) throw new ArgumentNullException(nameof(urlLock));

            Expression e = new Expression("Url", Operators.Equals, urlLock.Url);
            e.PrependAnd(new Expression("LockType", Operators.Equals, LockType.Write.ToString()));

            if (!String.IsNullOrEmpty(urlLock.UserGuid)) e.PrependAnd(new Expression("UserGuid", Operators.Equals, urlLock.UserGuid));

            _Database.Delete(LOCKS_TABLE, e);
        }

        public void RemoveWriteLock(string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            
            Expression e = new Expression("Url", Operators.Equals, url);
            e.PrependAnd(new Expression("LockType", Operators.Equals, LockType.Write.ToString()));

            if (!String.IsNullOrEmpty(userGuid)) e.PrependAnd(new Expression("UserGuid", Operators.Equals, userGuid));

            _Database.Delete(LOCKS_TABLE, e);
        }

        public bool WriteLockExists(string url)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));

            Expression e = new Expression("Url", Operators.Equals, url);
            e.PrependAnd(new Expression("LockType", Operators.Equals, LockType.Write.ToString()));
            DataTable result = _Database.Select(LOCKS_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0) return true;
            return false;
        }

        public List<UrlLock> GetWriteLocks()
        {
            List<UrlLock> ret = new List<UrlLock>();
            Expression e = new Expression("Id", Operators.GreaterThan, 0);
            e.PrependAnd(new Expression("LockType", Operators.Equals, LockType.Write.ToString()));
            DataTable result = _Database.Select(LOCKS_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(UrlLock.FromDataRow(row));
                }
            }
            return ret;
        }

        #endregion

        #region Container-Methods

        public bool AddContainer(Container container)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (ContainerExists(container.UserGuid, container.Name)) return false;

            if (String.IsNullOrEmpty(container.ObjectsDirectory))
            {
                container.ObjectsDirectory =
                    _Settings.Storage.Directory +
                    container.UserGuid + "/" +
                    container.GUID + "/";
            }

            Dictionary<string, object> insertVals = container.ToInsertDictionary();
            _Database.Insert(CONTAINERS_TABLE, insertVals);
            return true;
        }
         
        public void RemoveContainer(Container container)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));

            Expression e = new Expression("GUID", Operators.Equals, container.GUID);
            _Database.Delete(CONTAINERS_TABLE, e);
        }
         
        public bool ContainerExists(Container container)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));

            Expression e = new Expression("GUID", Operators.Equals, container.GUID);
            DataTable result = _Database.Select(CONTAINERS_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0) return true;
            return false;
        }

        public bool ContainerExists(string userGuid, string name)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            Expression e = new Expression("UserGuid", Operators.Equals, userGuid);
            e.PrependAnd(new Expression("Name", Operators.Equals, name));
            DataTable result = _Database.Select(CONTAINERS_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0) return true;
            return false;
        }

        public Container GetContainer(string userGuid, string name)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            Expression e = new Expression("UserGuid", Operators.Equals, userGuid);
            e.PrependAnd(new Expression("Name", Operators.Equals, name));
            DataTable result = _Database.Select(CONTAINERS_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                return Container.FromDataRow(result.Rows[0]);
            }
            return null;
        }

        public List<Container> GetContainers()
        {
            List<Container> ret = new List<Container>();
            Expression e = new Expression("Id", Operators.GreaterThan, 0);
            DataTable result = _Database.Select(CONTAINERS_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(Container.FromDataRow(row));
                }
            }

            return ret;
        }

        public List<Container> GetContainersByUser(string userGuid)
        {
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));

            List<Container> ret = new List<Container>();
            Expression e = new Expression("UserGuid", Operators.Equals, userGuid);
            DataTable result = _Database.Select(CONTAINERS_TABLE, null, null, null, e, null);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    ret.Add(Container.FromDataRow(row));
                }
            }

            return ret;
        }

        public void UpdateContainer(Container container)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(container.GUID)) throw new ArgumentException("No GUID found in container.");

            Dictionary<string, object> updateVals = container.ToInsertDictionary();
            Expression e = new Expression("GUID", Operators.Equals, container.GUID);
            _Database.Update(CONTAINERS_TABLE, updateVals, e);
        }

        #endregion

        #region Authentication-Methods

        public Permission GetEffectivePermissions(int? apiKeyId, int? userId)
        {
            if (apiKeyId == null && userId == null) throw new ArgumentException("Either API key ID or user ID must be supplied.");

            Permission ret = new Permission();
            ret.Id = 0;
            ret.GUID = Guid.NewGuid().ToString();
            ret.UserMasterId = userId;
            ret.ApiKeyId = apiKeyId;
            ret.Notes = "*** System-generated effective permissions ***"; 
            ret.ReadObject = false;
            ret.ReadContainer = false;
            ret.WriteObject = false;
            ret.WriteContainer = false;
            ret.DeleteObject = false;
            ret.DeleteContainer = false;
            ret.Active = false;
            ret.CreatedUtc = DateTime.Now.ToUniversalTime();

            if (apiKeyId != null)
            {
                List<Permission> apiKeyPermissions = GetPermissionsByApiKeyId(apiKeyId);
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

            if (userId != null)
            {
                List<Permission> userPermissions = GetPermissionsByUserId(userId);
                if (userPermissions != null && userPermissions.Count > 0)
                {
                    foreach (Permission perm in userPermissions)
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

        public bool Authenticate(
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

            user = GetUserById(apiKey.UserMasterId);
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

            effectivePermissions = GetEffectivePermissions(apiKey.Id, user.Id);
            result = AuthResult.Success;
            return true;
        }

        public bool Authenticate(
            string email,
            string password,
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

            if (String.IsNullOrEmpty(email)) return false;
            if (String.IsNullOrEmpty(password)) return false;

            user = GetUserByEmail(email);
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

            if (!password.Equals(user.Password))
            {
                result = AuthResult.InvalidCredentials;
                return false;
            }

            effectivePermissions = GetEffectivePermissions(null, user.Id);
            result = AuthResult.Success;
            return true;
        }

        #endregion

        #endregion

        #region Private-Methods

        private void InitializeTables()
        {
            List<string> tableNames = _Database.ListTables(); 
            tableNames = tableNames.ConvertAll(d => d.ToLower());

            if (!tableNames.Contains(USER_TABLE.ToLower()))
            {
                List<Column> userColumns = UserMaster.GetTableColumns();
                _Database.CreateTable(USER_TABLE, userColumns);
            }

            if (!tableNames.Contains(API_KEY_TABLE.ToLower()))
            {
                List<Column> apiKeyColumns = ApiKey.GetTableColumns();
                _Database.CreateTable(API_KEY_TABLE, apiKeyColumns);
            }

            if (!tableNames.Contains(PERMISSION_TABLE.ToLower()))
            {
                List<Column> permissionColumns = Permission.GetTableColumns(); 
                _Database.CreateTable(PERMISSION_TABLE, permissionColumns);
            }

            if (!tableNames.Contains(LOCKS_TABLE.ToLower()))
            {
                List<Column> locksColumns = UrlLock.GetTableColumns();
                _Database.CreateTable(LOCKS_TABLE, locksColumns);
            }

            if (!tableNames.Contains(CONTAINERS_TABLE.ToLower()))
            {
                List<Column> containerColumns = Container.GetTableColumns();
                _Database.CreateTable(CONTAINERS_TABLE, containerColumns);
            }
        }

        #endregion 
    }
}
