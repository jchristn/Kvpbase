using System;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

namespace Kvpbase
{
    public class DiskInfo
    {
        #region Public-Members

        public string Name { get; set; }
        public string VolumeLabel { get; set; }
        public string DriveFormat { get; set; }
        public string DriveType { get; set; }
        public long TotalSizeBytes { get; set; }
        public long TotalSizeGigabytes { get; set; }
        public long AvailableSizeBytes { get; set; }
        public long AvailableSizeGigabytes { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public DiskInfo()
        {

        }

        public static List<DiskInfo> GetAllDisks()
        {
            List<DiskInfo> ret = new List<DiskInfo>();

            foreach (DriveInfo drive in DriveInfo.GetDrives())
            {
                try
                {
                    DiskInfo curr = new DiskInfo();
                    curr.Name = drive.Name;

                    curr.VolumeLabel = drive.VolumeLabel;
                    curr.DriveFormat = drive.DriveFormat;
                    curr.DriveType = drive.DriveType.ToString();
                    curr.TotalSizeBytes = drive.TotalSize;
                    curr.TotalSizeGigabytes = drive.TotalSize / (1024 * 1024 * 1024);
                    curr.AvailableSizeBytes = drive.TotalFreeSpace;
                    curr.AvailableSizeGigabytes = drive.TotalFreeSpace / (1024 * 1024 * 1024);

                    ret.Add(curr);
                }
                catch (IOException)
                {
                    // do nothing, disk likely unvailable
                }
            }

            return ret;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
