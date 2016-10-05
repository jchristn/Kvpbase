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

        private ConcurrentDictionary<string, Tuple<int?, string, string, DateTime>> ActiveObjects; // UserMasterId, SourceIp, verb, established
        
        #endregion

        #region Constructors-and-Factories

        public UrlLockManager()
        {
            ActiveObjects = new ConcurrentDictionary<string, Tuple<int?, string, string, DateTime>>();
        }

        #endregion

        #region Public-Methods

        public Dictionary<string, Tuple<int?, string, string, DateTime>> GetLockedUrls()
        {
            Dictionary<string, Tuple<int?, string, string, DateTime>> ret;
            ret = ActiveObjects.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            return ret;
        }

        public bool LockUrl(RequestMetadata md)
        {
            bool success = false;

            if (md.CurrentUserMaster != null)
            {
                success = ActiveObjects.TryAdd(
                    md.CurrentObj.DiskPath,
                    new Tuple<int?, string, string, DateTime>(
                        md.CurrentUserMaster.UserMasterId,
                        md.CurrentHttpRequest.SourceIp,
                        md.CurrentHttpRequest.Method,
                        DateTime.Now
                    ));
            }
            else
            {
                success = ActiveObjects.TryAdd(
                    md.CurrentObj.DiskPath,
                    new Tuple<int?, string, string, DateTime>(
                        0,
                        md.CurrentHttpRequest.SourceIp,
                        md.CurrentHttpRequest.Method,
                        DateTime.Now
                    ));
            }

            return success;
        }

        public bool LockResource(RequestMetadata md, string resource)
        {
            bool success = ActiveObjects.TryAdd(
                resource,
                new Tuple<int?, string, string, DateTime>(
                    md.CurrentUserMaster.UserMasterId,
                    md.CurrentHttpRequest.SourceIp,
                    md.CurrentHttpRequest.Method,
                    DateTime.Now
                ));

            return success;
        }

        public bool UnlockUrl(RequestMetadata md)
        {
            Tuple<int?, string, string, DateTime> original;

            bool success = ActiveObjects.TryRemove(
                md.CurrentObj.DiskPath,
                out original);

            return success;
        }

        public bool UnlockResource(RequestMetadata md, string resource)
        {
            Tuple<int?, string, string, DateTime> original;

            bool success = ActiveObjects.TryRemove(
                resource,
                out original);

            return success;
        }

        #endregion
    }
}
