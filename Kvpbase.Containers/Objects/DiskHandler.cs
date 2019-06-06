using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

using Kvpbase.Core;

namespace Kvpbase.Containers
{
    /// <summary>
    /// Object handler using disk storage.
    /// </summary>
    internal class DiskHandler : ObjectHandler
    {
        #region Public-Members

        /// <summary>
        /// Buffer size to use when reading streams.
        /// </summary>
        public int StreamReadBufferSize
        {
            get
            {
                return _StreamReadBufferSize;
            }
            set
            {
                if (value < 1) throw new ArgumentException("StreamReadBufferSize must be greater than zero.");
                _StreamReadBufferSize = value;
            }
        }

        /// <summary>
        /// Buffer size to use when writing to a stream.
        /// </summary>
        public int StreamWriteBufferSize
        {
            get
            {
                return _StreamWriteBufferSize;
            }
            set
            {
                if (value < 1) throw new ArgumentException("StreamWriteBufferSize must be greater than zero.");
                _StreamWriteBufferSize = value;
            }
        }

        #endregion

        #region Private-Members

        private int _StreamReadBufferSize;
        private int _StreamWriteBufferSize;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public DiskHandler()
        {
            _StreamReadBufferSize = 65536;
            _StreamWriteBufferSize = 65536;
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
        /// Write an object.  The file will be re-read in its entirety to calculate the MD5.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="contentLength">The content length of the object.</param>
        /// <param name="stream">The stream containing the object data.</param>
        /// <param name="md5">The MD5 hash of the object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool Write(string fullFilename, long contentLength, Stream stream, out string md5, out ErrorCode error)
        {
            error = ErrorCode.None;
            md5 = null;

            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));
            if (contentLength < 0) throw new ArgumentException("Invalid content length.");
            if (stream == null || !stream.CanRead) throw new ArgumentException("Unable to read stream.");

            try
            {
                if (contentLength < 1)
                {
                    CreateEmptyFile(fullFilename);
                    return true;
                }
                else
                {
                    int bytesRead = 0;
                    byte[] buffer = new byte[_StreamReadBufferSize];  
                    long bytesRemaining = contentLength;

                    using (MemoryStream ms = new MemoryStream()) // so we can compute hash
                    {
                        using (FileStream fs = new FileStream(fullFilename, FileMode.OpenOrCreate))
                        {
                            while (bytesRemaining > 0)
                            {
                                bytesRead = stream.ReadAsync(buffer, 0, _StreamReadBufferSize).Result;
                                fs.Write(buffer, 0, bytesRead);
                                ms.Write(buffer, 0, bytesRead); // so we can compute the hash
                                bytesRemaining -= bytesRead;
                            }
                        }

                        md5 = Common.Md5(ms);
                    }

                    error = ErrorCode.Success;
                    return true;
                }
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
        /// Write bytes to a specific position in an existing object.  The file will be re-read in its entirety to calculate the MD5.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="position">The byte position to which data should be written.</param>
        /// <param name="contentLength">The number of bytes to write.</param>
        /// <param name="stream">The stream containing the data to write.</param>
        /// <param name="md5">The MD5 hash of the object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool WriteRange(string fullFilename, long position, long contentLength, Stream stream, out string md5, out ErrorCode error)
        {
            error = ErrorCode.None;
            md5 = null;

            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));
            if (position < 0) throw new ArgumentException("Position must be zero or greater");
            if (contentLength < 1) throw new ArgumentException("Invalid content length.");
            if (stream == null || !stream.CanRead) throw new ArgumentException("Unable to read stream.");

            try
            {
                using (FileStream fs = new FileStream(fullFilename, FileMode.OpenOrCreate))
                {
                    fs.Seek(position, SeekOrigin.Begin);

                    int bytesRead = 0;
                    byte[] buffer = new byte[_StreamReadBufferSize];
                    long bytesRemaining = contentLength;
                      
                    while (bytesRemaining > 0)
                    {
                        bytesRead = stream.ReadAsync(buffer, 0, _StreamReadBufferSize).Result;
                        fs.Write(buffer, 0, bytesRead); 
                        bytesRemaining -= bytesRead;
                    }

                    // compute hash of the entire file
                    if (fs.CanSeek) fs.Seek(0, SeekOrigin.Begin);
                    md5 = Common.Md5(fs);

                    error = ErrorCode.Success;
                    return true;
                }
            }
            catch (Exception e)
            { 
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
        /// Read an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="contentLength">The content length of the object.</param>
        /// <param name="stream">The stream containing the object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool Read(string fullFilename, out long contentLength, out Stream stream, out ErrorCode error)
        {
            error = ErrorCode.None;
            contentLength = 0;
            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));

            try
            {
                contentLength = GetFileSize(fullFilename);
                stream = new FileStream(fullFilename, FileMode.Open);
                error = ErrorCode.Success;
                return true;
            }
            catch (Exception e)
            {
                stream = null;
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

            if (position + count > GetFileSize(fullFilename))
            {
                data = null;
                error = ErrorCode.OutOfRange;
                return false;
            }

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
        /// Read bytes from a specific range in an existing object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="position">The byte position from which data should be read.</param>
        /// <param name="count">The number of bytes to read.</param>
        /// <param name="stream">The stream containing the object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public override bool ReadRange(string fullFilename, long position, int count, out Stream stream, out ErrorCode error)
        {
            error = ErrorCode.None;
            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));
            if (position < 0) throw new ArgumentException("Position must be greater than or equal to zero");
            if (count < 1) throw new ArgumentException("Count must be one or greater");

            if (position + count > GetFileSize(fullFilename))
            {
                stream = null;
                error = ErrorCode.OutOfRange;
                return false;
            }

            try
            {
                using (FileStream fs = new FileStream(fullFilename, FileMode.Open))
                {
                    stream = new MemoryStream();
                    fs.Seek(position, SeekOrigin.Begin);
                    int bytesRemaining = count;
                    int bytesRead = 0;
                    byte[] buffer = new byte[_StreamReadBufferSize];

                    while (bytesRemaining > 0)
                    {
                        bytesRead = fs.Read(buffer, 0, _StreamReadBufferSize);
                        stream.Write(buffer, 0, bytesRead);
                        bytesRemaining -= bytesRead;
                    }
                }

                if (stream.CanSeek) stream.Seek(0, SeekOrigin.Begin);
                error = ErrorCode.Success;
                return true;
            }
            catch (Exception e)
            {
                stream = null;
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
                size = GetFileSize(fullFilename);
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
                else ret = ErrorCode.IOError;
            }
            else if (e is FileNotFoundException)
            {
                ret = ErrorCode.NotFound;
            }
            else if (e is EndOfStreamException || e is ObjectDisposedException)
            {
                ret = ErrorCode.StreamError;
            }
            else if (e is ArgumentOutOfRangeException)
            {
                ret = ErrorCode.OutOfRange;
            }
            else
            {
                ret = ErrorCode.ServerError;
            }

            return ret;
        }
         
        private long GetFileSize(string fullFilename)
        {
            return new FileInfo(fullFilename).Length;
        }

        #endregion
    }
}
