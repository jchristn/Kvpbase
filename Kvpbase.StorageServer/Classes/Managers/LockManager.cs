using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
        private string _Header = "[Kvpbase.LockManager] ";

        private CancellationTokenSource _TokenSource = new CancellationTokenSource();
        private CancellationToken _Token;

        internal LockManager(Settings settings, LoggingModule logging, WatsonORM orm)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (orm == null) throw new ArgumentNullException(nameof(orm));

            _Settings = settings;
            _Logging = logging;
            _ORM = orm;
            _Token = _TokenSource.Token;

            Task.Run(() => MonitorForExpiredLocks(), _Token);
        }

        #region General-Methods

        internal bool LockExists(string url)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.Url)),
                DbOperators.Equals,
                url);
             
            UrlLock urlLock = _ORM.SelectFirst<UrlLock>(e);
            if (urlLock != null) return true;
            return false;
        }

        internal void RemoveLocks(string url)
        {
            if (String.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.Url)),
                DbOperators.Equals,
                url);

            _ORM.DeleteMany<UrlLock>(e);
        }

        #endregion

        #region Read-Lock-Methods

        /* 
         * Read operations cannot continue if the URL is being written, but can continue if being read elsewhere.
         *   
         */

        internal string AddReadLock(RequestMetadata md)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));

            if (WriteLockExists(md.Http.Request.Url.RawWithoutQuery)) return null;

            UrlLock urlLock = null;
            DateTime expirationUtc = DateTime.Now.ToUniversalTime().AddSeconds(_Settings.Storage.LockExpirationSeconds);
            if (md.Params.ExpirationUtc != null) expirationUtc = md.Params.ExpirationUtc.Value.ToUniversalTime();

            if (md.User != null && !String.IsNullOrEmpty(md.User.GUID))
            {
                urlLock = new UrlLock(LockType.Read, md.Http.Request.Url.RawWithoutQuery, md.User.GUID, expirationUtc);
            }
            else
            {
                urlLock = new UrlLock(LockType.Read, md.Http.Request.Url.RawWithoutQuery, null, expirationUtc);
            }

            urlLock = _ORM.Insert<UrlLock>(urlLock);
            return urlLock.GUID;
        }

        internal void RemoveReadLock(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.GUID)),
                DbOperators.Equals,
                guid);

            _ORM.DeleteMany<UrlLock>(e);
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

        internal string AddWriteLock(RequestMetadata md)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));

            if (WriteLockExists(md.Http.Request.Url.RawWithoutQuery)) return null;
            if (ReadLockExists(md.Http.Request.Url.RawWithoutQuery)) return null;

            UrlLock urlLock = null;
            DateTime expirationUtc = DateTime.Now.ToUniversalTime().AddSeconds(_Settings.Storage.LockExpirationSeconds);
            if (md.Params.ExpirationUtc != null) expirationUtc = md.Params.ExpirationUtc.Value.ToUniversalTime();

            if (md.User != null && !String.IsNullOrEmpty(md.User.GUID))
            {
                urlLock = new UrlLock(LockType.Write, md.Http.Request.Url.RawWithoutQuery, md.User.GUID, expirationUtc);
            }
            else
            {
                urlLock = new UrlLock(LockType.Write, md.Http.Request.Url.RawWithoutQuery, null, expirationUtc);
            }

            urlLock = _ORM.Insert<UrlLock>(urlLock);
            return urlLock.GUID;
        }

        internal void RemoveWriteLock(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<UrlLock>(nameof(UrlLock.GUID)),
                DbOperators.Equals,
                guid);

            _ORM.DeleteMany<UrlLock>(e);
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

        #region Private-Methods

        private void MonitorForExpiredLocks()
        {
            while (!_TokenSource.IsCancellationRequested)
            {
                DbExpression e = new DbExpression(
                    _ORM.GetColumnName<UrlLock>(nameof(UrlLock.ExpirationUtc)),
                    DbOperators.LessThan,
                    DateTime.Now.ToUniversalTime());

                e.PrependAnd(_ORM.GetColumnName<UrlLock>(nameof(UrlLock.ExpirationUtc)),
                    DbOperators.IsNotNull,
                    null);

                List<UrlLock> expired = _ORM.SelectMany<UrlLock>(e);
                if (expired != null && expired.Count > 0)
                {
                    foreach (UrlLock curr in expired)
                    {
                        _Logging.Info(_Header + "lock " + curr.GUID + " expired at " + curr.ExpirationUtc.ToString("s") + ", removing");
                        _ORM.Delete<UrlLock>(curr);
                    }
                }

                Task.Delay(10000).Wait();
            }
        }

        #endregion
    }
}
