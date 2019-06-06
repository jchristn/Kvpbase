using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Core;

namespace Kvpbase.Classes.Managers
{
    public class UrlLockManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private LoggingModule _Logging;

        private readonly object _WriteLock;
        private readonly object _ReadLock;
        private Dictionary<string, LockedResource> _WriteLockResources;
        private List<string> _ReadLockResources;

        private static string _TimestampFormat = "yyyy-MM-ddTHH:mm:ss.ffffffZ";

        #endregion

        #region Constructors-and-Factories

        public UrlLockManager(LoggingModule logging)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            _Logging = logging;

            _WriteLock = new object();
            _ReadLock = new object();

            _WriteLockResources = new Dictionary<string, LockedResource>();
            _ReadLockResources = new List<string>();
        }

        #endregion

        #region Public-Methods

        public Dictionary<string, LockedResource> GetWriteLockedUrls()
        {
            lock (_WriteLock)
            {
                return new Dictionary<string, LockedResource>(_WriteLockResources);
            }
        }

        public List<string> GetReadLockedUrls()
        {
            lock (_ReadLock)
            {
                return new List<string>(_ReadLockResources);
            }
        }
         
        public bool AddWriteLock(RequestMetadata md)
        {
            string key = KeyFromMetadata(md);

            lock (_WriteLock)
            {
                if (_WriteLockResources.ContainsKey(key))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "AddWriteLock resource currently locked for writing: " + key);
                    return false;
                }

                lock (_ReadLock)
                {
                    if (_ReadLockResources.Contains(key))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AddWriteLock resource currently being read: " + key);
                        return false;
                    }
                }

                _WriteLockResources.Add(key, new LockedResource(md.User, md.Http.Method, md.Http.RawUrlWithoutQuery));
                return true;
            }
        }

        public void RemoveWriteLock(RequestMetadata md)
        {
            string key = KeyFromMetadata(md);

            lock (_WriteLock)
            {
                if (_WriteLockResources.ContainsKey(key)) _WriteLockResources.Remove(key);
            }

            return;
        }

        public bool AddReadLock(RequestMetadata md)
        {
            string key = KeyFromMetadata(md);

            lock (_WriteLock)
            {
                if (_WriteLockResources.ContainsKey(key))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "AddReadLock resource currently locked for writing: " + key);
                    return false;
                }

                lock (_ReadLock)
                {
                    if (!_ReadLockResources.Contains(key)) _ReadLockResources.Add(key);
                }
                 
                return true;
            }
        }

        public void RemoveReadLock(RequestMetadata md)
        {
            string key = KeyFromMetadata(md);

            lock (_ReadLock)
            {
                if (_ReadLockResources.Contains(key)) _ReadLockResources.Remove(key);
            }

            return;
        }

        #endregion

        #region Private-Methods

        private string KeyFromMetadata(RequestMetadata md)
        {
            string ret = "";
            ret += md.Http.Method.ToString();
            ret += " ";
            ret += md.Http.RawUrlWithoutQuery.ToLower();
            ret += " ";
            ret += md.Http.TimestampUtc.ToString(_TimestampFormat);
            return ret;
        }

        #endregion
    }
}
