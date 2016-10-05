using System;
using System.Collections.Generic;
using System.Linq;
using SyslogLogging;

namespace Kvpbase
{
    public class ContainerPermission
    {
        #region Public-Members

        public int? UserMasterId { get; set; }
        public int? ApiKeyId { get; set; }
        public string Notes { get; set; }
        public int? AllowReadContainer { get; set; }
        public int? AllowReadObject { get; set; }
        public int? AllowWriteContainer { get; set; }
        public int? AllowWriteObject { get; set; }
        public int? AllowDeleteContainer { get; set; }
        public int? AllowDeleteObject { get; set; }
        public int? AllowSearch { get; set; }

        #endregion

        #region Constructors-and-Factories

        public ContainerPermission()
        {

        }

        public static ContainerPermission DefaultPermit()
        {
            ContainerPermission ret = new ContainerPermission();
            ret.UserMasterId = 0;
            ret.ApiKeyId = 0;
            ret.Notes = "Default permit";
            ret.AllowReadContainer = 1;
            ret.AllowReadObject = 1;
            ret.AllowWriteContainer = 1;
            ret.AllowWriteObject = 1;
            ret.AllowDeleteContainer = 1;
            ret.AllowDeleteObject = 1;
            ret.AllowSearch = 1;
            return ret;
        }

        #endregion

        #region Public-Static-Methods

        public static bool GetPermission(string operation, RequestMetadata md, ContainerPropertiesFile props)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(operation)) return false;
            if (md == null) return false;
            if (md.CurrentUserMaster == null) return false;
            if (md.CurrentApiKey == null) return false;
            if (props == null) return true;

            #endregion

            #region Set-Default

            bool defaultAllow = false;

            if (props.DefaultPermissionAllow != null) defaultAllow = Common.IsTrue(props.DefaultPermissionAllow);
            else if (props.DefaultPermissionDeny != null) defaultAllow = !Common.IsTrue(props.DefaultPermissionDeny);
            else defaultAllow = false;

            #endregion

            #region Override-Default-with-Specific

            List<ContainerPermission> permlist = new List<ContainerPermission>();
            bool AllowReadContainer = false;
            bool AllowReadObject = false;
            bool AllowWriteContainer = false;
            bool AllowWriteObject = false;
            bool AllowDeleteContainer = false;
            bool AllowDeleteObject = false;
            bool AllowSearch = false;

            foreach (ContainerPermission curr in props.Permissions)
            {
                if (md.CurrentUserMaster != null)
                {
                    if (md.CurrentUserMaster.UserMasterId == curr.UserMasterId)
                    {
                        permlist.Add(curr);
                        continue;
                    }
                }

                if (md.CurrentApiKey != null)
                {
                    if (md.CurrentApiKey.ApiKeyId == curr.ApiKeyId)
                    {
                        permlist.Add(curr);
                        continue;
                    }
                }

                if (curr.UserMasterId == 0 && curr.ApiKeyId == 0)
                {
                    permlist.Add(curr);
                    continue;
                }
            }

            if (permlist == null) return defaultAllow;
            if (permlist.Count < 1) return defaultAllow;
            permlist = permlist.Distinct().ToList();

            foreach (ContainerPermission curr in permlist)
            {
                AllowReadContainer = AllowReadContainer || Common.IsTrue(curr.AllowReadContainer);
                AllowReadObject = AllowReadObject || Common.IsTrue(curr.AllowReadObject);
                AllowWriteContainer = AllowWriteContainer || Common.IsTrue(curr.AllowWriteContainer);
                AllowWriteObject = AllowWriteObject || Common.IsTrue(curr.AllowWriteObject);
                AllowDeleteContainer = AllowDeleteContainer || Common.IsTrue(curr.AllowDeleteContainer);
                AllowDeleteObject = AllowDeleteObject || Common.IsTrue(curr.AllowDeleteObject);
                AllowSearch = AllowSearch || Common.IsTrue(curr.AllowSearch);
            }

            #endregion

            #region Process

            switch (operation)
            {
                case "ReadContainer":
                    return AllowReadContainer;

                case "ReadObject":
                    return AllowReadObject;

                case "WriteContainer":
                    return AllowWriteContainer;

                case "WriteObject":
                    return AllowWriteObject;

                case "DeleteContainer":
                    return AllowDeleteContainer;

                case "DeleteObject":
                    return AllowDeleteObject;

                case "Search":
                    return AllowSearch;

                default:
                    return false;
            }

            #endregion
        }

        #endregion
    }
}
