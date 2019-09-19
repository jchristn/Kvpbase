using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

using SyslogLogging;

using Kvpbase.Classes;

namespace Kvpbase.Containers
{ 
    internal class DiskDriver : StorageDriver
    {
        #region Public-Members
         
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

        private LoggingModule _Logging;

        #endregion

        #region Constructors-and-Factories
         
        public DiskDriver(LoggingModule logging)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));

            _StreamReadBufferSize = 65536;
            _StreamWriteBufferSize = 65536;

            _Logging = logging;
        }

        #endregion

        #region Public-Methods
         
        public override bool Exists(string fullFilename)
        {
            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));
            return File.Exists(fullFilename);
        }
          
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

                        ms.Seek(0, SeekOrigin.Begin);
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
          
        public override bool Read(string fullFilename, out long contentLength, out Stream stream, out ErrorCode error)
        {
            stream = null;
            error = ErrorCode.None;
            contentLength = 0;
            if (String.IsNullOrEmpty(fullFilename)) throw new ArgumentNullException(nameof(fullFilename));

            try
            {
                contentLength = GetFileSize(fullFilename); 
                stream = new FileStream(fullFilename, FileMode.Open, FileAccess.Read);

                /*
                stream = new MemoryStream();
                using (FileStream fs = new FileStream(fullFilename, FileMode.Open, FileAccess.Read))
                {
                    fs.CopyTo(stream);
                    stream.Seek(0, SeekOrigin.Begin);
                }
                 */

                /*
                using (FileStream fs = new FileStream(fullFilename, FileMode.Open, FileAccess.Read))
                {
                    stream = new MemoryStream();

                    long bytesRemaining = contentLength;
                    int bytesRead = 0;
                    byte[] buffer = null;

                    while (bytesRemaining > 0)
                    {
                        buffer = new byte[_StreamReadBufferSize];

                        bytesRead = fs.Read(buffer, 0, buffer.Length);
                        if (bytesRead > 0)
                        {
                            bytesRemaining -= bytesRead;
                            stream.Write(buffer, 0, bytesRead);
                        }
                    }

                    stream.Seek(0, SeekOrigin.Begin);
                } 
                 */

                error = ErrorCode.Success;
                return true;
            }
            catch (Exception e)
            {
                _Logging.Exception("DiskHandler", "Read", e);
                stream = null;
                error = SetErrorCode(e);
                return false;
            } 
        }
          
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

                    while (bytesRemaining > 0)
                    {
                        byte[] buffer = null;

                        if (bytesRemaining >= _StreamReadBufferSize)
                        {
                            buffer = new byte[_StreamReadBufferSize];
                        }
                        else
                        {
                            buffer = new byte[bytesRemaining];
                        }

                        bytesRead = fs.Read(buffer, 0, buffer.Length); 
                        stream.Write(buffer, 0, bytesRead);
                        bytesRemaining -= bytesRead;
                    }

                    stream.Seek(0, SeekOrigin.Begin);
                }

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
