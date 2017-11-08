using System;
using System.Collections.Generic;
using System.Linq;
using SyslogLogging;

namespace Kvpbase
{
    public class ObjectPermission
    {
        #region Public-Members

        public int? UserMasterId { get; set; }
        public int? ApiKeyId { get; set; }
        public string Notes { get; set; }
        public int? AllowReadObject { get; set; }
        public int? AllowWriteObject { get; set; }
        public int? AllowDeleteObject { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public ObjectPermission()
        {

        }

        public static ObjectPermission DefaultPermit()
        {
            ObjectPermission ret = new ObjectPermission();
            ret.UserMasterId = 0;
            ret.ApiKeyId = 0;
            ret.Notes = "Default permit";
            ret.AllowReadObject = 1;
            ret.AllowWriteObject = 1;
            ret.AllowDeleteObject = 1;
            return ret;
        }

        #endregion

        #region Public-Methods

        public static bool GetPermission(string operation, RequestMetadata md, ObjectPropertiesFile props)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(operation)) return false;
            if (md == null) return false;
            if (md.CurrUser == null) return false;
            if (md.CurrApiKey == null) return false;
            if (props == null) return true;

            #endregion

            #region Set-Default

            bool defaultAllow = false;

            if (props.DefaultPermissionAllow != null) defaultAllow = Common.IsTrue(props.DefaultPermissionAllow);
            else if (props.DefaultPermissionDeny != null) defaultAllow = !Common.IsTrue(props.DefaultPermissionDeny);
            else defaultAllow = false;

            #endregion

            #region Override-Default-with-Specific

            List<ObjectPermission> permlist = new List<ObjectPermission>();
            bool AllowReadObject = false;
            bool AllowWriteObject = false;
            bool AllowDeleteObject = false;

            foreach (ObjectPermission curr in props.Permissions)
            {
                if (md.CurrUser != null)
                {
                    if (md.CurrUser.UserMasterId == curr.UserMasterId)
                    {
                        permlist.Add(curr);
                        continue;
                    }
                }

                if (md.CurrApiKey != null)
                {
                    if (md.CurrApiKey.ApiKeyId == curr.ApiKeyId)
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

            foreach (ObjectPermission curr in permlist)
            {
                AllowReadObject = AllowReadObject || Common.IsTrue(curr.AllowReadObject);
                AllowWriteObject = AllowWriteObject || Common.IsTrue(curr.AllowWriteObject);
                AllowDeleteObject = AllowDeleteObject || Common.IsTrue(curr.AllowDeleteObject);
            }

            #endregion

            #region Process

            switch (operation)
            {
                case "ReadObject":
                    return AllowReadObject;

                case "WriteObject":
                    return AllowWriteObject;

                case "DeleteObject":
                    return AllowDeleteObject;

                default:
                    return false;
            }

            #endregion
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
