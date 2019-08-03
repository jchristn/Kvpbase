using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using SyslogLogging;

using Kvpbase.Core;

namespace Kvpbase.Classes.Managers
{
    public class TempFilesManager
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

        private Settings _Settings;
        private LoggingModule _Logging;

        private readonly object _Lock;
        private Dictionary<string, DateTime> _LockedGuids;

        private int _StreamReadBufferSize;
        private int _StreamWriteBufferSize;

        #endregion

        #region Constructors-and-Factories

        public TempFilesManager(Settings settings, LoggingModule logging)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Storage == null) throw new ArgumentNullException(nameof(settings.Storage));
            if (String.IsNullOrEmpty(settings.Storage.TempFiles)) throw new ArgumentNullException(nameof(settings.Storage.TempFiles));

            _Settings = settings;
            _Logging = logging;

            _Lock = new object();
            _LockedGuids = new Dictionary<string, DateTime>();

            _StreamReadBufferSize = 65536;
            _StreamWriteBufferSize = 65536;

            if (!Directory.Exists(_Settings.Storage.TempFiles)) Directory.CreateDirectory(_Settings.Storage.TempFiles);
            if (!_Settings.Storage.TempFiles.EndsWith("/")) _Settings.Storage.TempFiles += "/"; 
        }

        #endregion

        #region Public-Methods

        public bool Add(byte[] data, out string guid)
        {
            guid = null;
            
            guid = Guid.NewGuid().ToString();

            try
            {
                if (!AddLock(guid))
                {
                    guid = null;
                    _Logging.Log(LoggingModule.Severity.Warn, "TempFilesManager Add unable to acquire lock for key " + guid);
                    return false;
                }

                if (!Common.WriteFile(_Settings.Storage.TempFiles + guid, data))
                {
                    guid = null;
                    _Logging.Log(LoggingModule.Severity.Warn, "TempFilesManager Add unable to write tempfile of length " + data.Length);
                    return false;
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "TempFilesManager Add added temporary file " + guid);
                }

                return true;
            }
            finally
            {
                RemoveLock(guid);
            }
        }

        public bool Add(long contentLength, Stream stream, out string guid)
        {
            guid = null;
            if (stream == null || !stream.CanRead) throw new ArgumentException("Cannot read from supplied stream.");
            if (contentLength < 0) throw new ArgumentException("Content length must be zero or greater.");

            guid = Guid.NewGuid().ToString();

            try
            {
                if (!AddLock(guid))
                {
                    guid = null;
                    _Logging.Log(LoggingModule.Severity.Warn, "TempFilesManager Add unable to acquire lock for key " + guid);
                    return false;
                }

                int bytesRead = 0;
                long bytesRemaining = contentLength;
                byte[] readBuffer = new byte[_StreamReadBufferSize];

                using (FileStream fs = new FileStream(_Settings.Storage.TempFiles + guid, FileMode.OpenOrCreate))
                {
                    while (bytesRemaining > 0)
                    {
                        bytesRead = stream.Read(readBuffer, 0, readBuffer.Length);

                        if (bytesRead > 0)
                        {
                            fs.Write(readBuffer, 0, bytesRead);
                            bytesRemaining -= bytesRead;
                        }
                    }
                }

                return true;
            }
            finally
            {
                RemoveLock(guid);
            }
        }

        public bool aRead(string guid, out byte[] data)
        {
            data = null;
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            try
            {
                if (!AddLock(guid))
                {
                    guid = null;
                    _Logging.Log(LoggingModule.Severity.Warn, "TempFilesManager Read unable to acquire lock for key " + guid);
                    return false;
                }

                data = Common.ReadBinaryFile(_Settings.Storage.TempFiles + guid);
                return true;
            }
            finally
            {
                RemoveLock(guid);
            }
        }

        public bool GetStream(string guid, out long contentLength, out Stream stream)
        {
            contentLength = 0;
            stream = new MemoryStream();

            try
            {
                if (!AddLock(guid))
                {
                    guid = null;
                    _Logging.Log(LoggingModule.Severity.Warn, "TempFilesManager Read unable to acquire lock for key " + guid);
                    return false;
                }

                contentLength = new FileInfo(_Settings.Storage.TempFiles + guid).Length;
                stream = new FileStream(_Settings.Storage.TempFiles + guid, FileMode.Open);
                return true;
            }
            finally
            {
                RemoveLock(guid);
            }
        }

        public bool Delete(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            try
            {
                if (!AddLock(guid))
                { 
                    _Logging.Log(LoggingModule.Severity.Warn, "TempFilesManager Delete unable to acquire lock for key " + guid);
                    return false;
                }

                if (File.Exists(_Settings.Storage.TempFiles + guid))
                {
                    File.Delete(_Settings.Storage.TempFiles + guid);
                }

                return true;
            }
            finally
            {
                RemoveLock(guid);
            }
        }

        #endregion

        #region Private-Methods

        private bool AddLock(string key)
        {
            lock (_Lock)
            {
                if (_LockedGuids.ContainsKey(key))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "TempFilesManager AddLock already exists for key " + key);
                    return false;
                }

                _LockedGuids.Add(key, DateTime.Now);
                return true;
            }
        }

        private void RemoveLock(string key)
        {
            lock (_Lock)
            {
                if (_LockedGuids.ContainsKey(key)) _LockedGuids.Remove(key);
            }

            return;
        }

        #endregion
    }
}
