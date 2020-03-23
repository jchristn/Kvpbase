using System;
using System.Collections.Generic;
using System.Text;
using DatabaseWrapper;
using SyslogLogging;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer.Classes.Managers
{
    internal class LockManager
    {
        private Settings _Settings;
        private LoggingModule _Logging;
        private DatabaseManager _Database;
        // private string _Header = "[Kvpbase.LockManager] ";

        internal LockManager(Settings settings, LoggingModule logging, DatabaseManager database)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (database == null) throw new ArgumentNullException(nameof(database));

            _Settings = settings;
            _Logging = logging;
            _Database = database;
        }

        #region Read-Lock-Methods

        /* 
         * Read operations cannot continue if the URL is being written, but can continue if being read elsewhere.
         *   
         */

        internal bool AddReadLock(RequestMetadata md)
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

        internal bool AddReadLock(string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));

            UrlLock urlLock = new UrlLock(LockType.Read, url, userGuid);
            return AddReadLock(urlLock);
        }

        internal bool AddReadLock(UrlLock urlLock)
        {
            if (urlLock == null) throw new ArgumentNullException(nameof(urlLock));
            if (WriteLockExists(urlLock.Url)) return false;
            _Database.Insert<UrlLock>(urlLock);
            return true;
        }

        internal void RemoveReadLock(RequestMetadata md)
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

        internal void RemoveReadLock(UrlLock urlLock)
        {
            if (urlLock == null) throw new ArgumentNullException(nameof(urlLock));
            Expression e = new Expression("url", Operators.Equals, urlLock.Url);
            e.PrependAnd(new Expression("locktype", Operators.Equals, urlLock.LockType.ToString()));
            List<UrlLock> locks = _Database.SelectMany<UrlLock>(null, null, e, "ORDER BY id DESC");
            if (locks != null && locks.Count > 0)
            {
                foreach (UrlLock curr in locks)
                    _Database.Delete<UrlLock>(curr);
            }
        }

        internal void RemoveReadLock(string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            Expression e = new Expression("url", Operators.Equals, url);
            e.PrependAnd(new Expression("userguid", Operators.Equals, userGuid));
            List<UrlLock> locks = _Database.SelectMany<UrlLock>(null, null, e, "ORDER BY id DESC");
            if (locks != null && locks.Count > 0)
            {
                foreach (UrlLock curr in locks)
                    _Database.Delete<UrlLock>(curr);
            }
        }

        internal bool ReadLockExists(string url)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            Expression e = new Expression("url", Operators.Equals, url);
            e.PrependAnd(new Expression("locktype", Operators.Equals, LockType.Read.ToString()));
            UrlLock urlLock = _Database.SelectByFilter<UrlLock>(e, "ORDER BY id DESC");
            if (urlLock != null) return true;
            return false;
        }

        internal List<UrlLock> GetReadLocks()
        {
            Expression e = new Expression("locktype", Operators.Equals, LockType.Read.ToString());
            return _Database.SelectMany<UrlLock>(null, null, e, "ORDER BY id DESC"); 
        }

        #endregion

        #region Write-Lock-Methods

        /* 
         * Write operations cannot continue if the URL is being read or written elsewhere.
         *  
         */

        internal bool AddWriteLock(RequestMetadata md)
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

        internal bool AddWriteLock(string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid)); 
            UrlLock urlLock = new UrlLock(LockType.Write, url, userGuid);
            return AddWriteLock(urlLock);
        }

        internal bool AddWriteLock(UrlLock urlLock)
        {
            if (urlLock == null) throw new ArgumentNullException(nameof(urlLock));
            if (WriteLockExists(urlLock.Url)) return false;
            if (ReadLockExists(urlLock.Url)) return false;
            _Database.Insert<UrlLock>(urlLock);
            return true;
        }

        internal void RemoveWriteLock(RequestMetadata md)
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

        internal void RemoveWriteLock(UrlLock urlLock)
        {
            if (urlLock == null) throw new ArgumentNullException(nameof(urlLock)); 
            Expression e = new Expression("url", Operators.Equals, urlLock.Url);
            e.PrependAnd(new Expression("locktype", Operators.Equals, LockType.Write.ToString())); 
            if (!String.IsNullOrEmpty(urlLock.UserGUID)) e.PrependAnd(new Expression("userguid", Operators.Equals, urlLock.UserGUID));
            List<UrlLock> urlLocks = _Database.SelectMany<UrlLock>(null, null, e, "ORDER BY id DESC");
            if (urlLocks != null && urlLocks.Count > 0)
            {
                foreach (UrlLock curr in urlLocks)
                    _Database.Delete<UrlLock>(curr);
            }
        }

        internal void RemoveWriteLock(string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));

            Expression e = new Expression("url", Operators.Equals, url);
            e.PrependAnd(new Expression("locktype", Operators.Equals, LockType.Write.ToString())); 
            if (!String.IsNullOrEmpty(userGuid)) e.PrependAnd(new Expression("userguid", Operators.Equals, userGuid));
            List<UrlLock> urlLocks = _Database.SelectMany<UrlLock>(null, null, e, "ORDER BY id DESC");
            if (urlLocks != null && urlLocks.Count > 0)
            {
                foreach (UrlLock curr in urlLocks)
                    _Database.Delete<UrlLock>(curr);
            }
        }

        internal bool WriteLockExists(string url)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            Expression e = new Expression("url", Operators.Equals, url);
            e.PrependAnd(new Expression("locktype", Operators.Equals, LockType.Write.ToString()));
            UrlLock urlLock = _Database.SelectByFilter<UrlLock>(e, "ORDER BY id DESC");
            if (urlLock != null) return true;
            return false;
        }

        internal List<UrlLock> GetWriteLocks()
        {
            Expression e = new Expression("locktype", Operators.Equals, LockType.Write.ToString());
            return _Database.SelectMany<UrlLock>(null, null, e, "ORDER BY id DESC");
        }

        #endregion 
    }
}
