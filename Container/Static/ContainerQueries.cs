using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqliteWrapper;

namespace Kvpbase.Container
{
    /// <summary>
    /// Sqlite querybuilders for the container.
    /// </summary>
    internal static class ContainerQueries
    {
        #region Public-Methods

        public static string CreateObjectsTableQuery()
        {
            string query =
                "CREATE TABLE IF NOT EXISTS Objects " +
                "(" +
                "  Id                INTEGER PRIMARY KEY, " +
                "  Key               VARCHAR(256), " +
                "  ContentType       VARCHAR(128), " +
                "  ContentLength     INTEGER, " +
                "  Md5               VARCHAR(32), " +
                "  Tags              VARCHAR(256), " +
                "  CreatedUtc        VARCHAR(32), " +
                "  LastUpdateUtc     VARCHAR(32), " +
                "  LastAccessUtc     VARCHAR(32) " +
                ")";
            return query;
        }

        public static string CreateAuditLogTableQuery()
        {
            string query =
                "CREATE TABLE IF NOT EXISTS AuditLog " +
                "(" +
                "  Id                INTEGER PRIMARY KEY, " +
                "  Key               VARCHAR(256), " +
                "  Action            VARCHAR(32), " +
                "  Metadata          VARCHAR(1024), " +
                "  CreatedUtc        VARCHAR(32) " +
                ")";
            return query;
        }

        public static string SetLastAccess(string key, string ts)
        {
            string query =
                "UPDATE Objects SET LastAccessUtc = '" + Sanitize(ts) + "' " + 
                "WHERE Key = '" + Sanitize(key) + "'";
            return query;
        }

        public static string SetLastUpdate(string key, string ts)
        {
            string query =
                "UPDATE Objects SET LastUpdateUtc = '" + Sanitize(ts) + "' " +
                "WHERE Key = '" + Sanitize(key) + "'";
            return query;
        }

        public static string SetObjectSize(string key, long size)
        {
            string query =
                "UPDATE Objects SET ContentLength = '" + size + "' " +
                "WHERE Key = '" + Sanitize(key) + "'";
            return query;
        }

        public static string SetMd5(string key, string md5)
        {
            string query =
                "UPDATE Objects SET Md5 = '" + Sanitize(md5) + "' " +
                "WHERE Key = '" + Sanitize(key) + "'";
            return query;
        }

        public static string SetTags(string key, string tags)
        {
            string query =
                "UPDATE Objects SET Tags = '" + Sanitize(tags) + "' " +
                "WHERE Key = '" + Sanitize(key) + "'";
            return query;
        }

        public static string GetMd5(string key)
        {
            string query =
                "SELECT Md5 FROM Objects WHERE Key = '" + Sanitize(key) + "'";
            return query;
        }

        public static string GetLatestEntry()
        {
            string query =
                "SELECT LastUpdateUTC " +
                "FROM Objects " +
                "ORDER BY LastUpdateUtc DESC " +
                "LIMIT 1";
            return query;
        }

        public static string ReadObject(string key)
        {
            string query =
                "SELECT * FROM Objects WHERE key = '" + Sanitize(key) + "'";
            return query;
        }

        public static string WriteObject(string key, string contentType, long contentLength, string md5, List<string> tags, string ts)
        {
            return WriteObject(key, contentType, contentLength, md5, Common.StringListToCsv(tags), ts);
        }

        public static string WriteObject(string key, string contentType, long contentLength, string md5, string tags, string ts)
        {
            string query =
                "INSERT INTO Objects (Key, ContentType, ContentLength, Md5, Tags, CreatedUtc, LastUpdateUtc, LastAccessUtc) " +
                "VALUES " +
                "(" +
                "  '" + Sanitize(key) + "', " +
                "  '" + Sanitize(contentType) + "', " +
                "  '" + contentLength + "', " +
                "  '" + Sanitize(md5) + "', " +
                "  '" + Sanitize(tags) + "', " +
                "  '" + Sanitize(ts) + "', " +
                "  '" + Sanitize(ts) + "', " +
                "  '" + Sanitize(ts) + "' " +
                ")";
            return query;
        }

        public static string WriteObject(ObjectMetadata md)
        {
            string query =
                "INSERT INTO Objects (Key, ContentType, ContentLength, Md5, Tags, CreatedUtc, LastUpdateUtc, LastAccessUtc) " +
                "VALUES " +
                "(" +
                "  '" + Sanitize(md.Key) + "', " +
                "  '" + Sanitize(md.ContentType) + "', " +
                "  '" + md.ContentLength + "', " +
                "  '" + Sanitize(md.Md5) + "', " +
                "  '" + Sanitize(md.Tags) + "', " +
                "  '" + TimestampUtc(md.CreatedUtc) + "', " +
                "  '" + TimestampUtc(md.LastUpdateUtc) + "', " +
                "  '" + TimestampUtc(md.LastAccessUtc) + "' " +
                ")";
            return query;
        }

        public static string RemoveObject(string key)
        {
            string query =
                "DELETE FROM Objects WHERE key = '" + Sanitize(key) + "'";
            return query;
        }

        public static string RenameObject(string original, string updated, string ts)
        {
            string query =
                "UPDATE " +
                "  Objects " +
                "SET " +
                "  Key = '" + Sanitize(updated) + "', " +
                "  LastUpdateUtc = '" + Sanitize(ts) + "', " +
                "  LastAccessUtc = '" + Sanitize(ts) + "' " +
                "WHERE " +
                "  Key = '" + Sanitize(original) + "'";
            return query;
        }

        public static string ObjectCount()
        {
            return "SELECT COUNT(*) AS NumObjects FROM Objects";
        }

        public static string BytesConsumed()
        {
            return "SELECT SUM(ContentLength) AS Bytes FROM Objects";
        }

        public static string Enumerate(int? indexStart, int? maxResults, EnumerationFilter filter, string orderByClause)
        {
            if (String.IsNullOrEmpty(orderByClause)) orderByClause = "ORDER BY CreatedUtc DESC";
            if (indexStart != null && indexStart < 1) indexStart = null;
            if (maxResults != null && maxResults < 1) maxResults = null;
            if (maxResults != null && maxResults > 1000) maxResults = 1000;

            string query =
                "SELECT * FROM Objects ";
              
            if (filter != null)
            {
                query += "WHERE Id > 0 ";

                if (filter.CreatedAfter != null)
                { 
                    query += "AND CreatedUtc > '" + TimestampUtc(Convert.ToDateTime(filter.CreatedAfter)) + "' "; 
                }

                if (filter.CreatedBefore != null)
                {
                    query += "AND CreatedUtc < '" + TimestampUtc(Convert.ToDateTime(filter.CreatedBefore)) + "' ";
                }

                if (filter.UpdatedAfter != null)
                {
                    query += "AND LastUpdateUtc > '" + TimestampUtc(Convert.ToDateTime(filter.UpdatedAfter)) + "' ";
                }

                if (filter.UpdatedBefore != null)
                {
                    query += "AND LastUpdateUtc < '" + TimestampUtc(Convert.ToDateTime(filter.UpdatedBefore)) + "' ";
                }

                if (filter.LastAccessAfter != null)
                {
                    query += "AND LastAccessUtc > '" + TimestampUtc(Convert.ToDateTime(filter.LastAccessAfter)) + "' ";
                }

                if (filter.LastAccessBefore != null)
                {
                    query += "AND LastAccessUtc < '" + TimestampUtc(Convert.ToDateTime(filter.LastAccessBefore)) + "' ";
                }

                if (!String.IsNullOrEmpty(filter.Md5)) 
                {
                    query += "AND Md5 = '" + Sanitize(filter.Md5) + "' ";
                }

                if (filter.SizeMin != null)
                {
                    query += "AND ContentLength >= '" + filter.SizeMin + "' ";
                }

                if (filter.SizeMax != null)
                {
                    query += "AND ContentLength <= '" + filter.SizeMax + "' ";
                } 

                if (filter.Tags != null && filter.Tags.Count > 0)
                {
                    foreach (string currTag in filter.Tags)
                    {
                        query += "AND Tags LIKE '%" + currTag + "%' ";
                    }
                }
            }

            query += orderByClause + " ";

            if (indexStart == null && maxResults == null)
            {
                query += "LIMIT 100";
            }
            else
            {
                if (indexStart == null && maxResults != null)
                {
                    query += "LIMIT " + maxResults;
                }
                else if (indexStart != null && maxResults != null)
                {
                    query += "LIMIT " + maxResults + " OFFSET " + indexStart;
                }
            }
             
            return query;
        }

        public static string AddAuditEntry(string key, AuditLogEntryType action, string metadata, string ts)
        {
            string query =
                "INSERT INTO AuditLog (Key, Action, Metadata, CreatedUtc) " +
                "VALUES " +
                "(" +
                "  '" + Sanitize(key) + "', " +
                "  '" + Sanitize(action.ToString()) + "', " +
                "  '" + Sanitize(metadata) + "', " +
                "  '" + Sanitize(ts) + "' " +
                ")";
            return query; 
        }

        public static string GetAuditEntries(string key, string action, int? maxResults, int? index, DateTime? createdBefore, DateTime? createdAfter)
        {
            if (maxResults == null || maxResults > 100) maxResults = 100;

            string query =
                "SELECT * FROM AuditLog " +
                "WHERE Id > 0 ";
            
            if (!String.IsNullOrEmpty(key)) query += "AND Key = '" + Sanitize(key) + "' ";
            if (!String.IsNullOrEmpty(action)) query += "AND Action = '" + Sanitize(action) + "' ";
            if (createdBefore != null) query += "AND CreatedUtc <= '" + TimestampUtc(Convert.ToDateTime(createdBefore)) + "' ";
            if (createdAfter != null) query += "AND CreatedUtc >= '" + TimestampUtc(Convert.ToDateTime(createdAfter)) + "' ";

            if (maxResults != null && index != null)
            {
                query += "LIMIT " + maxResults + " OFFSET " + index;
            }
            else if (maxResults != null)
            {
                query += "LIMIT " + maxResults;
            }

            return query;
        }

        public static string ClearAuditLog()
        {
            string query =
                "DELETE FROM AuditLog";
            return query;
        }

        #endregion

        #region Private-Methods

        private static string _TimestampFormat = "yyyy-MM-ddTHH:mm:ss.ffffffZ";

        private static string Sanitize(string str)
        {
            return DatabaseClient.SanitizeString(str);
        }

        private static string TimestampUtc()
        {
            return DateTime.Now.ToUniversalTime().ToString(_TimestampFormat);
        }

        private static string TimestampUtc(DateTime? ts)
        {
            return Convert.ToDateTime(ts).ToUniversalTime().ToString(_TimestampFormat);
        }

        private static string TimestampUtc(DateTime ts)
        {
            return ts.ToUniversalTime().ToString(_TimestampFormat);
        }

        #endregion
    }
}
