using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using SyslogLogging;
using DatabaseWrapper;
using Kvpbase.StorageServer.Classes;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer.Classes.Managers
{
    internal class DatabaseManager
    {
        private Settings _Settings;
        private LoggingModule _Logging;
        private DatabaseClient _Database;
        private static string _Header = "[Kvpbase.DatabaseManager] ";

        internal const string API_KEY_TABLE = "kvpbase_apikeys";
        internal const string USER_TABLE = "kvpbase_users";
        internal const string PERMISSION_TABLE = "kvpbase_permissions";
        internal const string LOCKS_TABLE = "kvpbase_urllocks";
        internal const string CONTAINERS_TABLE = "kvpbase_containers";
        internal const string OBJECTS_TABLE = "kvpbase_objects";
        internal const string CONTAINERS_KVP_TABLE = "kvpbase_container_metadata";
        internal const string OBJECTS_KVP_TABLE = "kvpbase_object_metadata";
        internal const string AUDIT_LOG_TABLE = "kvpbase_auditlog";
         
        internal DatabaseManager(Settings settings, LoggingModule logging, DatabaseClient database)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (database == null) throw new ArgumentNullException(nameof(database));

            _Settings = settings;
            _Logging = logging;
            _Database = database;

            if (_Settings.Debug.Database)
            {
                _Database.Logger = Logger;
                _Database.LogQueries = true;
                _Database.LogResults = true;
            }

            InitializeTables();
        }

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

            if (!tableNames.Contains(OBJECTS_TABLE.ToLower()))
            {
                List<Column> objectColumns = ObjectMetadata.GetTableColumns();
                _Database.CreateTable(OBJECTS_TABLE, objectColumns);
            }

            if (!tableNames.Contains(CONTAINERS_KVP_TABLE.ToLower()))
            {
                List<Column> containerMetadataColumns = ContainerKeyValuePair.GetTableColumns();
                _Database.CreateTable(CONTAINERS_KVP_TABLE, containerMetadataColumns);
            }

            if (!tableNames.Contains(OBJECTS_KVP_TABLE.ToLower()))
            {
                List<Column> objectMetadataColumns = ObjectKeyValuePair.GetTableColumns();
                _Database.CreateTable(OBJECTS_KVP_TABLE, objectMetadataColumns);
            }

            if (!tableNames.Contains(AUDIT_LOG_TABLE.ToLower()))
            {
                List<Column> auditLogColumns = AuditLogEntry.GetTableColumns();
                _Database.CreateTable(AUDIT_LOG_TABLE, auditLogColumns);
            } 
        }

        #region Primitives

        private string DatabaseTableNameFromObject(object obj)
        {
            if (obj == null) throw new ArgumentNullException(nameof(obj));
            if (obj is ApiKey) return API_KEY_TABLE;
            else if (obj is UserMaster) return USER_TABLE;
            else if (obj is Permission) return PERMISSION_TABLE;
            else if (obj is UrlLock) return LOCKS_TABLE;
            else if (obj is Container) return CONTAINERS_TABLE;
            else if (obj is ObjectMetadata) return OBJECTS_TABLE;
            else if (obj is ContainerKeyValuePair) return CONTAINERS_KVP_TABLE;
            else if (obj is ObjectKeyValuePair) return OBJECTS_KVP_TABLE;
            else if (obj is AuditLogEntry) return AUDIT_LOG_TABLE;
            throw new ArgumentException("Unknown object type: " + obj.GetType().ToString());
        }

        private string DatabaseTableNameFromObjectType(string type)
        {
            if (String.IsNullOrEmpty(type)) throw new ArgumentNullException(nameof(type));

            switch (type)
            {
                case "ApiKey":
                    return API_KEY_TABLE;
                case "UserMaster":
                    return USER_TABLE;
                case "Permission":
                    return PERMISSION_TABLE;
                case "UrlLock":
                    return LOCKS_TABLE;
                case "Container":
                    return CONTAINERS_TABLE;
                case "ObjectMetadata":
                    return OBJECTS_TABLE;
                case "ContainerKeyValuePair":
                    return CONTAINERS_KVP_TABLE;
                case "ObjectKeyValuePair":
                    return OBJECTS_KVP_TABLE;
                case "AuditLogEntry":
                    return AUDIT_LOG_TABLE;
                default:
                    throw new ArgumentException("Unknown type: " + type);
            }
        }

        private Dictionary<string, object> ObjectToInsertDictionary(object obj)
        {
            if (obj == null) throw new ArgumentNullException(nameof(obj));
            if (obj is ApiKey) return ((ApiKey)obj).ToInsertDictionary();
            else if (obj is UserMaster) return ((UserMaster)obj).ToInsertDictionary();
            else if (obj is Permission) return ((Permission)obj).ToInsertDictionary();
            else if (obj is UrlLock) return ((UrlLock)obj).ToInsertDictionary();
            else if (obj is Container) return ((Container)obj).ToInsertDictionary();
            else if (obj is ObjectMetadata) return ((ObjectMetadata)obj).ToInsertDictionary();
            else if (obj is ContainerKeyValuePair) return ((ContainerKeyValuePair)obj).ToInsertDictionary();
            else if (obj is ObjectKeyValuePair) return ((ObjectKeyValuePair)obj).ToInsertDictionary();
            else if (obj is AuditLogEntry) return ((AuditLogEntry)obj).ToInsertDictionary(); 
            throw new ArgumentException("Unknown object type: " + obj.GetType().ToString());
        }

        private T DataTableToObject<T>(DataTable result) where T : class
        {
            if (result == null || result.Rows.Count < 1) return null;
            if (typeof(T) == typeof(ApiKey)) return ApiKey.FromDataRow(result.Rows[0]) as T;
            else if (typeof(T) == typeof(UserMaster)) return UserMaster.FromDataRow(result.Rows[0]) as T;
            else if (typeof(T) == typeof(Permission)) return Permission.FromDataRow(result.Rows[0]) as T;
            else if (typeof(T) == typeof(UrlLock)) return UrlLock.FromDataRow(result.Rows[0]) as T;
            else if (typeof(T) == typeof(Container)) return Container.FromDataRow(result.Rows[0]) as T;
            else if (typeof(T) == typeof(ObjectMetadata)) return ObjectMetadata.FromDataRow(result.Rows[0]) as T;
            else if (typeof(T) == typeof(ContainerKeyValuePair)) return ContainerKeyValuePair.FromDataRow(result.Rows[0]) as T;
            else if (typeof(T) == typeof(ObjectKeyValuePair)) return ObjectKeyValuePair.FromDataRow(result.Rows[0]) as T;
            else if (typeof(T) == typeof(AuditLogEntry)) return AuditLogEntry.FromDataRow(result.Rows[0]) as T; 
            throw new ArgumentException("Unknown object type: " + typeof(T).Name);
        }

        private List<T> DataTableToListObject<T>(DataTable result)
        {
            if (result == null || result.Rows.Count < 1) return new List<T>();
            if (typeof(T) == typeof(ApiKey)) return ApiKey.FromDataTable(result) as List<T>;
            else if (typeof(T) == typeof(UserMaster)) return UserMaster.FromDataTable(result) as List<T>;
            else if (typeof(T) == typeof(Permission)) return Permission.FromDataTable(result) as List<T>;
            else if (typeof(T) == typeof(UrlLock)) return UrlLock.FromDataTable(result) as List<T>;
            else if (typeof(T) == typeof(Container)) return Container.FromDataTable(result) as List<T>;
            else if (typeof(T) == typeof(ObjectMetadata)) return ObjectMetadata.FromDataTable(result) as List<T>;
            else if (typeof(T) == typeof(ContainerKeyValuePair)) return ContainerKeyValuePair.FromDataTable(result) as List<T>;
            else if (typeof(T) == typeof(ObjectKeyValuePair)) return ObjectKeyValuePair.FromDataTable(result) as List<T>;
            else if (typeof(T) == typeof(AuditLogEntry)) return AuditLogEntry.FromDataTable(result) as List<T>; 
            throw new ArgumentException("Unknown object type: " + typeof(T).Name);
        }

        private int IdValFromObject(object entry, string tableName)
        {
            if (entry == null) throw new ArgumentNullException(nameof(entry));
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (tableName.Equals(API_KEY_TABLE)) return Convert.ToInt32(((ApiKey)entry).Id);
            else if (tableName.Equals(USER_TABLE)) return Convert.ToInt32(((UserMaster)entry).Id);
            else if (tableName.Equals(PERMISSION_TABLE)) return Convert.ToInt32(((Permission)entry).Id);
            else if (tableName.Equals(LOCKS_TABLE)) return Convert.ToInt32(((UrlLock)entry).Id);
            else if (tableName.Equals(CONTAINERS_TABLE)) return Convert.ToInt32(((Container)entry).Id);
            else if (tableName.Equals(OBJECTS_TABLE)) return Convert.ToInt32(((ObjectMetadata)entry).Id);
            else if (tableName.Equals(CONTAINERS_KVP_TABLE)) return Convert.ToInt32(((ContainerKeyValuePair)entry).Id);
            else if (tableName.Equals(OBJECTS_KVP_TABLE)) return Convert.ToInt32(((ObjectKeyValuePair)entry).Id);
            else if (tableName.Equals(AUDIT_LOG_TABLE)) return Convert.ToInt32(((AuditLogEntry)entry).Id);
            throw new ArgumentException("Unknown table: " + tableName);
        }

        internal T Insert<T>(T entry) where T : class
        {
            if (entry == null) throw new ArgumentNullException(nameof(entry));
            string tableName = DatabaseTableNameFromObject(entry);
            Dictionary<string, object> insertVals = ObjectToInsertDictionary(entry);
            DataTable result = _Database.Insert(tableName, insertVals);
            return DataTableToObject<T>(result);
        }

        internal T Update<T>(T entry) where T : class
        {
            if (entry == null) throw new ArgumentNullException(nameof(entry));
            string tableName = DatabaseTableNameFromObject(entry); 
            int id = IdValFromObject(entry, tableName);
            Dictionary<string, object> updateVals = ObjectToInsertDictionary(entry);
            Expression e = new Expression("id", Operators.Equals, id);
            _Database.Update(tableName, updateVals, e);
            DataTable result = _Database.Select(tableName, null, null, null, e, null);
            return DataTableToObject<T>(result);
        }
         
        internal void Delete<T>(T entry)
        {
            if (entry == null) throw new ArgumentNullException(nameof(entry));
            string table = DatabaseTableNameFromObject(entry); 
            int id = IdValFromObject(entry, table);
            Expression e = new Expression("id", Operators.Equals, id);
            _Database.Delete(table, e);
        }
         
        internal void DeleteById<T>(int id)
        {
            string tableName = DatabaseTableNameFromObjectType(typeof(T).Name); 
            Expression e = new Expression("id", Operators.Equals, id);
            _Database.Delete(tableName, e);
        }

        internal void DeleteByGUID<T>(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            string tableName = DatabaseTableNameFromObjectType(typeof(T).Name);
            Expression e = new Expression("guid", Operators.Equals, guid);
            _Database.Delete(tableName, e);
        }

        internal void DeleteByFilter<T>(Expression e)
        {
            if (e == null) throw new ArgumentNullException(nameof(e));
            string tableName = DatabaseTableNameFromObjectType(typeof(T).Name);
            _Database.Delete(tableName, e);
        }

        internal DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expression filter, string orderByClause)
        {
            return _Database.Select(tableName, indexStart, maxResults, returnFields, filter, orderByClause);
        }
         
        internal T SelectById<T>(int id) where T : class
        {
            string tableName = DatabaseTableNameFromObjectType(typeof(T).Name); 
            Expression e = new Expression("id", Operators.Equals, id);
            DataTable result = _Database.Select(tableName, null, null, null, e, null);
            if (result == null || result.Rows.Count < 1) return null;
            return DataTableToObject<T>(result);
        }
         
        internal T SelectByGUID<T>(string guid) where T : class
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            string tableName = DatabaseTableNameFromObjectType(typeof(T).Name);
            Expression e = new Expression("guid", Operators.Equals, guid);
            DataTable result = _Database.Select(tableName, null, null, null, e, null);
            if (result == null || result.Rows.Count < 1) return null;
            return DataTableToObject<T>(result);
        }
         
        internal T SelectByFilter<T>(Expression e, string orderByClause) where T : class
        {
            if (e == null) throw new ArgumentNullException(nameof(e));
            if (String.IsNullOrEmpty(orderByClause)) throw new ArgumentNullException(nameof(orderByClause));
            string tableName = DatabaseTableNameFromObjectType(typeof(T).Name); 
            e.PrependAnd("id", Operators.GreaterThan, 0);
            DataTable result = _Database.Select(tableName, null, 1, null, e, orderByClause);
            if (result == null || result.Rows.Count < 1) return null;
            return DataTableToObject<T>(result);
        }
         
        internal List<T> SelectMany<T>(int? startIndex, int? maxResults, Expression e, string orderByClause)
        {
            if (String.IsNullOrEmpty(orderByClause)) throw new ArgumentNullException(nameof(orderByClause));
            string tableName = DatabaseTableNameFromObjectType(typeof(T).Name); 
            if (e == null) e = new Expression("id", Operators.GreaterThan, 0);
            else e.PrependAnd(new Expression("id", Operators.GreaterThan, 0));
            DataTable result = _Database.Select(tableName, startIndex, maxResults, null, e, orderByClause);
            if (result == null || result.Rows.Count < 1) return new List<T>();
            return DataTableToListObject<T>(result);
        }
         
        internal DataTable Query(string query)
        {
            return _Database.Query(query);
        }
         
        internal string Sanitize(string str)
        {
            if (String.IsNullOrEmpty(str)) return null;
            return _Database.SanitizeString(str);
        }
         
        internal string Timestamp(DateTime dt)
        {
            return _Database.Timestamp(dt);
        }

        private void Logger(string msg)
        {
            _Logging.Debug(_Header + msg);
        }

        #endregion 
    }
}
