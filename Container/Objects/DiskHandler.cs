using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kvpbase
{
    /// <summary>
    /// Object handler using disk storage.
    /// </summary>
    internal class DiskHandler : ObjectHandler
    {
        #region Public-Members

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public DiskHandler()
        {

        }

        #endregion

        #region Public-Methods
         
        /// <summary>
        /// Check if an object exists.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <returns>True if exists.</returns>
        public override bool Exists(string fullFilename)
        {
            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));
            return File.Exists(fullFilename);
        }

        /// <summary>
        /// Write an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="data">The object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool Write(string fullFilename, byte[] data, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));

            try
            {
                if (data == null || data.Length < 1) CreateEmptyFile(fullFilename);
                else File.WriteAllBytes(fullFilename, data);
                error = ErrorCode.Success;
                return true;
            }
            catch (Exception e)
            {
                error = SetErrorCode(e);
                return false;
            }
        }

        /// <summary>
        /// Write bytes to a specific position in an existing object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="position">The byte position to which data should be written.</param>
        /// <param name="data">The data to write.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool WriteRange(string fullFilename, long position, byte[] data, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));
            if (position < 0) throw new ArgumentException("Position must be zero or greater");
            if (data == null || data.Length < 1) return true;

            try
            {
                using (Stream stream = new FileStream(fullFilename, FileMode.OpenOrCreate))
                {
                    stream.Seek(position, SeekOrigin.Begin);
                    stream.Write(data, 0, data.Length);
                    error = ErrorCode.Success;
                    return true;
                }
            }
            catch (Exception e)
            {
                data = null;
                error = SetErrorCode(e);
                return false;
            }
        }

        /// <summary>
        /// Read an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="data">The object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool Read(string fullFilename, out byte[] data, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));

            try
            {
                data = File.ReadAllBytes(fullFilename);
                error = ErrorCode.Success;
                return true;
            }
            catch (Exception e)
            {
                data = null;
                error = SetErrorCode(e);
                return false;
            }
        }

        /// <summary>
        /// Read bytes from a specific range in an existing object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="position">The byte position from which data should be read.</param>
        /// <param name="count">The number of bytes to read.</param>
        /// <param name="data">The object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool ReadRange(string fullFilename, long position, int count, out byte[] data, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));
            if (position < 0) throw new ArgumentException("Position must be greater than or equal to zero");
            if (count < 1) throw new ArgumentException("Count must be one or greater");

            try
            {
                data = new byte[count];

                using (Stream stream = new FileStream(fullFilename, FileMode.Open))
                {
                    stream.Seek(position, SeekOrigin.Begin);
                    stream.Read(data, 0, count);
                    error = ErrorCode.Success;
                    return true;
                }
            }
            catch (Exception e)
            {
                data = null;
                error = SetErrorCode(e);
                return false;
            }
        }

        /// <summary>
        /// Delete an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool Delete(string fullFilename, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));

            try
            {
                File.Delete(fullFilename);
                return true;
            }
            catch (Exception e)
            {
                error = SetErrorCode(e);
                return false;
            }
        }

        /// <summary>
        /// Rename an object.
        /// </summary>
        /// <param name="original">The full path and filename of the object.</param>
        /// <param name="updated">The desired full path and filename of the object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool Rename(string original, string updated, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(original)) throw new ArgumentNullException(nameof(original));
            if (String.IsNullOrEmpty(updated)) throw new ArgumentNullException(nameof(updated));

            if (!File.Exists(original))
            {
                error = ErrorCode.NotFound;
                return false;
            }

            if (File.Exists(updated))
            {
                error = ErrorCode.AlreadyExists;
                return false;
            }

            try
            {
                File.Move(original, updated);
                return true;
            }
            catch (Exception e)
            {
                error = SetErrorCode(e);
                return false;
            }
        }

        /// <summary>
        /// Retrieve the size of an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="size">The size of the object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool GetObjectSize(string fullFilename, out long size, out ErrorCode error)
        {
            error = ErrorCode.None;
            size = 0;

            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));

            try
            {
                size = new FileInfo(fullFilename).Length;
                return true;
            }
            catch (Exception e)
            {
                error = SetErrorCode(e);
                return false;
            }
        }

        #endregion

        #region Private-Methods

        private void CreateEmptyFile(string filename)
        {
            File.Create(filename).Dispose();
        }

        private bool IsDiskFull(Exception ex)
        {
            const int HR_ERROR_HANDLE_DISK_FULL = unchecked((int)0x80070027);
            const int HR_ERROR_DISK_FULL = unchecked((int)0x80070070);

            return ex.HResult == HR_ERROR_HANDLE_DISK_FULL
                || ex.HResult == HR_ERROR_DISK_FULL;
        }

        private ErrorCode SetErrorCode(Exception e)
        {
            ErrorCode ret = ErrorCode.None;

            if (e == null) return ret;

            if (e is DirectoryNotFoundException)
            {
                ret = ErrorCode.NotFound;
            }
            else if (e is UnauthorizedAccessException || e is IOException)
            {
                if (IsDiskFull(e)) ret = ErrorCode.DiskFull;
                else ret = ErrorCode.FileAccessError;
            }
            else if (e is FileNotFoundException)
            {
                ret = ErrorCode.NotFound;
            }
            else
            {
                ret = ErrorCode.ServerError;
            }

            return ret;
        }

        #endregion
    }
}
