using System;
using System.Collections.Generic;
using SyslogLogging;

namespace Kvpbase
{
    public class ContainerPropertiesFile
    {
        #region Public-Members

        public string Notes { get; set; }
        public ContainerLogging Logging { get; set; }
        public int? DefaultPermissionAllow { get; set; }
        public int? DefaultPermissionDeny { get; set; }
        public List<ContainerPermission> Permissions { get; set; }

        #endregion

        #region Constructors-and-Factories

        public ContainerPropertiesFile()
        {

        }

        public static ContainerPropertiesFile FromObject(Obj currObj, out string logFilePath, out string propertiesFilePath)
        {
            logFilePath = null;
            propertiesFilePath = null;

            #region Check-for-Null-Values

            if (currObj == null) return null;
            if (String.IsNullOrEmpty(currObj.DiskPath)) return null;

            #endregion

            #region Variables

            logFilePath = String.Copy(currObj.DiskPath) + ".container.log." + DateTime.Now.ToUniversalTime().ToString("MMddyyyy");
            propertiesFilePath = String.Copy(currObj.DiskPath) + ".container.properties";
            string propfileContents = "";
            ContainerPropertiesFile ret = new ContainerPropertiesFile();

            #endregion

            #region Remove-Last-Instance-of-Key

            if (!String.IsNullOrEmpty(currObj.Key))
            {
                if (propertiesFilePath.Contains(currObj.Key))
                {
                    int keyIndex = propertiesFilePath.LastIndexOf(currObj.Key);
                    propertiesFilePath = propertiesFilePath.Remove(keyIndex, currObj.Key.Length);
                }

                if (logFilePath.Contains(currObj.Key))
                {
                    int keyIndex = logFilePath.LastIndexOf(currObj.Key);
                    logFilePath = logFilePath.Remove(keyIndex, currObj.Key.Length);
                }
            }

            #endregion

            #region Process

            if (Common.FileExists(propertiesFilePath))
            {
                // properties exist, read and deserialize
                propfileContents = Common.ReadTextFile(propertiesFilePath);
                if (String.IsNullOrEmpty(propfileContents)) return null;

                try
                {
                    ret = Common.DeserializeJson<ContainerPropertiesFile>(propfileContents);
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
