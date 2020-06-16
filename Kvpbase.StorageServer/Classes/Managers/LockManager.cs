using System;
using System.Collections.Generic;
using System.Text;
using SyslogLogging;
using Watson.ORM;
using Watson.ORM.Core;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer.Classes.Managers
{
    internal class LockManager
    {
        private Settings _Settings;
        private LoggingModule _Logging;
        private WatsonORM _ORM;
        // private string _Header = "[Kvpbase.LockManager] ";

        internal LockManager(Settings settings, LoggingModule logging, WatsonORM orm)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (orm == null) throw new ArgumentNullException(nameof(orm));

            _Settings = settings;
            _Logging = logging;
            _ORM = orm;
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
            _ORM.Insert<UrlLock>(urlLock);
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

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.Url)),
                DbOperators.Equals,
                urlLock.Url);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.LockType)),
                DbOperators.Equals,
                urlLock.LockType));

            List<UrlLock> locks = _ORM.SelectMany<UrlLock>(e);
            if (locks != null && locks.Count > 0)
            {
                foreach (UrlLock curr in locks)
                    _ORM.Delete<UrlLock>(curr);
            }
        }

        internal void RemoveReadLock(string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.Url)),
                DbOperators.Equals,
                url);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.UserGUID)),
                DbOperators.Equals,
                userGuid));

            List<UrlLock> locks = _ORM.SelectMany<UrlLock>(e);
            if (locks != null && locks.Count > 0)
            {
                foreach (UrlLock curr in locks)
                    _ORM.Delete<UrlLock>(curr);
            }
        }

        internal bool ReadLockExists(string url)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.Url)),
                DbOperators.Equals,
                url);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.LockType)),
                DbOperators.Equals,
                LockType.Read));

            UrlLock urlLock = _ORM.SelectFirst<UrlLock>(e);
            if (urlLock != null) return true;
            return false;
        }

        internal List<UrlLock> GetReadLocks()
        {
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.Id)),
                DbOperators.GreaterThan,
                0);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.LockType)),
                DbOperators.Equals,
                LockType.Read));

            return _ORM.SelectMany<UrlLock>(e); 
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
            _ORM.Insert<UrlLock>(urlLock);
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

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.Url)),
                DbOperators.Equals,
                urlLock.Url);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.LockType)),
                DbOperators.Equals,
                LockType.Write));

            if (!String.IsNullOrEmpty(urlLock.UserGUID))
            {
                e.PrependAnd(new DbExpression(
                    _ORM.GetColumnName<UrlLock>(nameof(UrlLock.UserGUID)),
                    DbOperators.Equals,
                    urlLock.UserGUID)); 
            }

            List<UrlLock> urlLocks = _ORM.SelectMany<UrlLock>(e);
            if (urlLocks != null && urlLocks.Count > 0)
            {
                foreach (UrlLock curr in urlLocks)
                    _ORM.Delete<UrlLock>(curr);
            }
        }

        internal void RemoveWriteLock(string url, string userGuid)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.Url)),
                DbOperators.Equals,
                url);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.LockType)),
                DbOperators.Equals,
                LockType.Write));

            if (!String.IsNullOrEmpty(userGuid))
            {
                e.PrependAnd(new DbExpression(
                    _ORM.GetColumnName<UrlLock>(nameof(UrlLock.UserGUID)),
                    DbOperators.Equals,
                    userGuid));
            }
             
            List<UrlLock> urlLocks = _ORM.SelectMany<UrlLock>(e);
            if (urlLocks != null && urlLocks.Count > 0)
            {
                foreach (UrlLock curr in urlLocks)
                    _ORM.Delete<UrlLock>(curr);
            }
        }

        internal bool WriteLockExists(string url)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.Url)),
                DbOperators.Equals,
                url);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.LockType)),
                DbOperators.Equals,
                LockType.Write));
             
            UrlLock urlLock = _ORM.SelectFirst<UrlLock>(e);
            if (urlLock != null) return true;
            return false;
        }

        internal List<UrlLock> GetWriteLocks()
        {
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.Id)),
                DbOperators.GreaterThan,
                0);

            e.PrependAnd(new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.LockType)),
                DbOperators.Equals,
                LockType.Write));

            return _ORM.SelectMany<UrlLock>(e);
        }

        #endregion 
    }
}
