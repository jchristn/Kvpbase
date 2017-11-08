using System;
using System.IO;
using SyslogLogging;

namespace Kvpbase
{
    public class ObjInfo
    {
        #region Public-Members

        public string Key { get; set; }
        public long Size { get; set; }
        public DateTime? Created { get; set; }
        public DateTime? LastUpdate { get; set; }
        public DateTime? LastAccess { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public ObjInfo()
        {

        }

        public static ObjInfo FromFile(string file)
        {
            if (String.IsNullOrEmpty(file)) return null;
            if (!File.Exists(file)) return null;

            ObjInfo ret = new ObjInfo();
            FileInfo fi = new FileInfo(file);

            ret.Size = fi.Length;
            ret.Created = fi.CreationTimeUtc;
            ret.LastUpdate = fi.LastWriteTimeUtc;
            ret.LastAccess = fi.LastAccessTimeUtc;
            ret.Key = fi.Name;

            return ret;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
