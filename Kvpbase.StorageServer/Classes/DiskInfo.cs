using System;
using System.Collections.Generic;
using System.IO; 

namespace Kvpbase.Classes
{
    /// <summary>
    /// Information about attached disks.
    /// </summary>
    public class DiskInfo
    {
        #region Public-Members

        /// <summary>
        /// The name of the disk, i.e. 'C:\\'.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The volume label for the disk.
        /// </summary>
        public string VolumeLabel { get; set; }

        /// <summary>
        /// The format of the disk, i.e. NTFS.
        /// </summary>
        public string DriveFormat { get; set; }

        /// <summary>
        /// The type of the disk, i.e. 'Fixed'.
        /// </summary>
        public string DriveType { get; set; }

        /// <summary>
        /// The total size of the disk, in bytes.
        /// </summary>
        public long TotalSizeBytes { get; set; }

        /// <summary>
        /// The total size of the disk, in gigabytes.
        /// </summary>
        public long TotalSizeGigabytes { get; set; }

        /// <summary>
        /// The amount of free space on the disk, in bytes.
        /// </summary>
        public long AvailableSizeBytes { get; set; }

        /// <summary>
        /// The amount of free space on the disk, in gigabytes.
        /// </summary>
        public long AvailableSizeGigabytes { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public DiskInfo()
        {

        }

        /// <summary>
        /// Retrieve information about all attached disks.
        /// </summary>
        /// <returns>List of DiskInfo.</returns>
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
