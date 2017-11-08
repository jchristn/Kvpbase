using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public class UrlLockManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Events _Logging;
        private Dictionary<string, Tuple<int?, string, string, DateTime>> _LockedObjects;            // UserMasterId, SourceIp, verb, established
        private List<string> _ReadObjects;                                                           // URL
        private readonly object _Lock;

        #endregion

        #region Constructors-and-Factories

        public UrlLockManager(Events logging)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            _Logging = logging;
            _LockedObjects = new Dictionary<string, Tuple<int?, string, string, DateTime>>();
            _ReadObjects = new List<string>();
            _Lock = new object();
        }

        #endregion

        #region Public-Methods

        public Dictionary<string, Tuple<int?, string, string, DateTime>> GetLockedUrls()
        {
            lock (_Lock)
            {
                return _LockedObjects;
            }
        }

        public List<string> GetReadUrls()
        {
            lock (_Lock)
            {
                return _ReadObjects;
            }
        }

        public bool LockUrl(RequestMetadata md)
        {
            lock (_Lock)
            {
                if (_LockedObjects.ContainsKey(md.CurrObj.DiskPath))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "LockUrl resource currently locked: " + md.CurrObj.DiskPath);
                    return false;
                }

                if (_ReadObjects.Contains(md.CurrObj.DiskPath))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "LockUrl resource currently being read: " + md.CurrObj.DiskPath);
                    return false;
                }

                _LockedObjects.Add(md.CurrObj.DiskPath,
                    new Tuple<int?, string, string, DateTime>(
                        md.CurrUser.UserMasterId,
                        md.CurrHttpReq.SourceIp,
                        md.CurrHttpReq.Method,
                        DateTime.Now));
            }

            return true;
        }

        public bool LockResource(RequestMetadata md, string resource)
        {
            lock (_Lock)
            {
                if (_LockedObjects.ContainsKey(md.CurrObj.DiskPath))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "LockResource resource currently locked: " + md.CurrObj.DiskPath);
                    return false;
                }

                if (_ReadObjects.Contains(md.CurrObj.DiskPath))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "LockResource resource currently being read: " + md.CurrObj.DiskPath);
                    return false;
                }

                _LockedObjects.Add(resource,
                    new Tuple<int?, string, string, DateTime>(
                        md.CurrUser.UserMasterId,
                        md.CurrHttpReq.SourceIp,
                        md.CurrHttpReq.Method,
                        DateTime.Now));
            }

            return true;
        }

        public bool UnlockUrl(RequestMetadata md)
        {
            lock (_Lock)
            {
                if (_LockedObjects.ContainsKey(md.CurrObj.DiskPath))
                {
                    _LockedObjects.Remove(md.CurrObj.DiskPath);
                }
            }

            return true;
        }

        public bool UnlockResource(RequestMetadata md, string resource)
        {
            lock (_Lock)
            {
                if (_LockedObjects.ContainsKey(resource))
                {
                    _LockedObjects.Remove(resource);
                }
            }

            return true;
        }

        public bool AddReadResource(string resource)
        {
            lock (_Lock)
            {
                if (_LockedObjects.ContainsKey(resource))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "AddReadResource resource currently being read: " + resource);
                    return false;
                }

                _ReadObjects.Add(resource);
            }

            return true;
        }

        public bool RemoveReadResource(string resource)
        {
            lock (_Lock)
            {
                if (_ReadObjects.Contains(resource))
                {
                    _ReadObjects.Remove(resource);
                }
            }

            return true;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
