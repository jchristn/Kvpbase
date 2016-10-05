using System;
using System.Collections.Generic;
using SyslogLogging;

namespace Kvpbase
{
    public class ObjectPropertiesFile
    {
        #region Public-Members

        public string Notes { get; set; }
        public ObjectLogging Logging { get; set; }
        public int? DefaultPermissionAllow { get; set; }
        public int? DefaultPermissionDeny { get; set; }
        public List<ObjectPermission> Permissions { get; set; }

        #endregion

        #region Constructors-and-Factories

        public ObjectPropertiesFile()
        {

        }

        public static ObjectPropertiesFile FromObject(Obj currObj, out string logFilePath, out string propertiesFilePath)
        {
            logFilePath = null;
            propertiesFilePath = null;

            #region Check-for-Null-Values

            if (currObj == null) return null;
            if (String.IsNullOrEmpty(currObj.Key)) return null;
            if (String.IsNullOrEmpty(currObj.DiskPath)) return null;

            #endregion

            #region Variables

            propertiesFilePath = String.Copy(currObj.DiskPath);
            int lastIndex = -1;
            string propFileContents = "";
            ObjectPropertiesFile ret = new ObjectPropertiesFile();

            #endregion

            #region Process

            lastIndex = propertiesFilePath.ToLower().LastIndexOf(currObj.Key.ToLower());
            if (lastIndex >= 0)
            {
                logFilePath = propertiesFilePath.Remove(lastIndex, currObj.Key.Length).Insert(lastIndex, "." + currObj.Key + ".log." + DateTime.Now.ToUniversalTime().ToString("MMddyyyy"));
                propertiesFilePath = propertiesFilePath.Remove(lastIndex, currObj.Key.Length).Insert(lastIndex, "." + currObj.Key + ".properties");
            }
            else
            {
                logFilePath = null;
                propertiesFilePath = null;
                return null;
            }

            if (Common.FileExists(propertiesFilePath))
            {
                // properties exist, read and deserialize
                propFileContents = Common.ReadTextFile(propertiesFilePath);
                if (String.IsNullOrEmpty(propFileContents)) return null;

                try
                {
                    ret = Common.DeserializeJson<ObjectPropertiesFile>(propFileContents);
                }
                catch (Exception)
                {
                    return null;
                }

                return ret;
            }
            else
            {
                propertiesFilePath = null;
                return null;
            }

            #endregion
        }

        #endregion
    }
}
