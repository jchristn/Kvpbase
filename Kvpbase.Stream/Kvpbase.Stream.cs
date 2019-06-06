using System;
using System.Collections.Generic;
using System.IO;
using System.Linq; 
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KvpbaseSDK;

namespace KvpbaseStreamProxy
{
    /// <summary>
    /// Stream interface for an object stored on Kvpbase v3.1 or greater.
    /// </summary>
    public class KvpbaseStream : Stream, IDisposable
    {
        #region Public-Members

        /// <summary>
        /// Object metadata.
        /// </summary>
        public ObjectMetadata Metadata
        {
            get
            {
                return _ObjectMetadata;
            }
            private set
            {
                _ObjectMetadata = value;
            }
        }

        #endregion

        #region Private-Members

        private bool _Disposed = false;

        private KvpbaseClient _Kvpbase;
        private string _ApiKey;
        private string _EndpointUrl;
        private string _UserGuid;
        private string _Container;
        private string _ObjectKey;

        private long _Position = 0;
        private long _Length = 0;

        private ObjectMetadata _ObjectMetadata;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the stream.
        /// </summary>
        /// <param name="apiKey">API key.</param>
        /// <param name="endpointUrl">Kvpbase node endpoint, i.e. http://hostname:port.</param>
        /// <param name="userGuid">The user GUID.</param>
        /// <param name="container">The container in which the object is stored.</param>
        /// <param name="objectKey">The object key.</param>
        public KvpbaseStream(string apiKey, string endpointUrl, string userGuid, string container, string objectKey)
        {
            if (String.IsNullOrEmpty(apiKey)) throw new ArgumentNullException(nameof(apiKey));
            if (String.IsNullOrEmpty(endpointUrl)) throw new ArgumentNullException(nameof(endpointUrl));
            if (String.IsNullOrEmpty(userGuid)) throw new ArgumentNullException(nameof(userGuid));
            if (String.IsNullOrEmpty(container)) throw new ArgumentNullException(nameof(container));
            if (String.IsNullOrEmpty(objectKey)) throw new ArgumentNullException(nameof(objectKey));

            _Kvpbase = new KvpbaseClient(userGuid, apiKey, endpointUrl);
            _ApiKey = apiKey;
            _EndpointUrl = endpointUrl;
            _UserGuid = userGuid;
            _Container = container;
            _ObjectKey = objectKey;

            if (!_Kvpbase.ObjectExists(_Container, _ObjectKey))
            {
                if (!_Kvpbase.WriteObject(_Container, _ObjectKey, "application/octet-stream", null))
                {
                    throw new IOException("Unable to create object.");
                }
            }

            if (!GetObjectMetadata())
            {
                throw new IOException("Unable to read object metadata.");
            }
            else
            {
                _Position = 0;
            }
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        private bool GetObjectMetadata()
        {
            bool success = _Kvpbase.GetObjectMetadata(_Container, _ObjectKey, out _ObjectMetadata);
            if (success)
            {
                _Length = Convert.ToInt64(_ObjectMetadata.ContentLength);
            }
            return success;
        }

        #endregion

        #region Stream-Override-Methods

        /// <summary>
        /// Indicates whether or not the stream can be read.  Always true.
        /// </summary>
        public override bool CanRead => true;

        /// <summary>
        /// Indicates whether or not the position in the stream can be modified using seek.  Always true.
        /// </summary>
        public override bool CanSeek => true;

        /// <summary>
        /// Indicates whether or not the stream can timeout.  Always false.
        /// </summary>
        public override bool CanTimeout => false;

        /// <summary>
        /// Indicates whether or not the stream can be written.  Always true.
        /// </summary>
        public override bool CanWrite => true;

        /// <summary>
        /// Indicates the length of the object.
        /// </summary>
        public override long Length => Convert.ToInt64(_ObjectMetadata.ContentLength);

        /// <summary>
        /// Indicates the current position in the stream.
        /// </summary>
        public override long Position
        {
            get
            {
                return _Position;
            }
            set
            {
                _Position = value;
            }
        }

        /// <summary>
        /// The read timeout.  Do not use.
        /// </summary>
        public override int ReadTimeout
        {
            get
            {
                return base.ReadTimeout;
            }
            set
            {
                base.ReadTimeout = value;
            }
        }

        /// <summary>
        /// The write timeout.  Do not use.
        /// </summary>
        public override int WriteTimeout
        {
            get
            {
                return base.WriteTimeout;
            }
            set
            {
                base.WriteTimeout = value;
            }
        }

        /// <summary>
        /// Unsupported async read.
        /// </summary> 
        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            // return base.BeginRead(buffer, offset, count, callback, state);
            throw new NotImplementedException();
        }

        /// <summary>
        /// Unsupported async write.
        /// </summary> 
        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            // return base.BeginWrite(buffer, offset, count, callback, state);
            throw new NotImplementedException();
        }

        /// <summary>
        /// Close the stream.
        /// </summary>
        public override void Close()
        {
            base.Close();
        }

        /// <summary>
        /// Unsupported method.
        /// </summary> 
        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            // return base.CopyToAsync(destination, bufferSize, cancellationToken);
            throw new NotImplementedException();
        }
         
        /// <summary>
        /// Unsupported method.
        /// </summary>
        public override int EndRead(IAsyncResult asyncResult)
        {
            // return base.EndRead(asyncResult);
            throw new NotImplementedException();
        }

        /// <summary>
        /// Unsupported method.
        /// </summary>
        public override void EndWrite(IAsyncResult asyncResult)
        {
            // base.EndWrite(asyncResult);
            throw new NotImplementedException();
        }

        /// <summary>
        /// Unsupported method.
        /// </summary>
        public override bool Equals(object obj)
        {
            // return base.Equals(obj);
            throw new NotImplementedException();
        }

        /// <summary>
        /// Unsupported method.
        /// </summary>
        public override void Flush()
        {
            return;
        }

        /// <summary>
        /// Unsupported method.
        /// </summary>
        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            // return base.FlushAsync(cancellationToken);
            throw new NotImplementedException();
        }

        /// <summary>
        /// Unsupported method.
        /// </summary>
        public override int GetHashCode()
        {
            // return base.GetHashCode();
            throw new NotImplementedException();
        }

        /// <summary>
        /// Unsupported method.
        /// </summary>
        public override object InitializeLifetimeService()
        {
            // return base.InitializeLifetimeService();
            throw new NotImplementedException();
        }

        /// <summary>
        /// Read from the stream.
        /// </summary>
        /// <param name="buffer">Buffer into which data should be read.</param>
        /// <param name="offset">Offset within the buffer to begin writing read data.</param>
        /// <param name="count">Number of bytes to read.</param>
        /// <returns>Number of bytes read.</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            if (offset > buffer.Length) throw new ArgumentOutOfRangeException(nameof(offset));
            if (offset + count > buffer.Length) throw new ArgumentOutOfRangeException(nameof(count));

            byte[] readData = new byte[count];

            if (!_Kvpbase.ReadObjectRange(_Container, _ObjectKey, _Position, count, out readData))
            {
                throw new IOException("Unable to read data.");
            }

            _Position += count;
            Buffer.BlockCopy(readData, 0, buffer, offset, readData.Length);
            return count;
        }

        /// <summary>
        /// Read asynchronously from the stream.
        /// </summary>        
        /// <param name="buffer">Buffer into which data should be read.</param>
        /// <param name="offset">Offset within the buffer to begin writing read data.</param>
        /// <param name="count">Number of bytes to read.</param> 
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Task with int indicating number of bytes read.</returns>
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            if (offset > buffer.Length) throw new ArgumentOutOfRangeException(nameof(offset));
            if (offset + count > buffer.Length) throw new ArgumentOutOfRangeException(nameof(count));

            return Task.Run(() =>
            {
                byte[] readData = new byte[count];

                if (!_Kvpbase.ReadObjectRange(_Container, _ObjectKey, _Position, count, out readData))
                {
                    throw new IOException("Unable to read data.");
                }

                _Position += count;
                Buffer.BlockCopy(readData, 0, buffer, offset, count);
                return count;

            }, cancellationToken);
        }

        /// <summary>
        /// Read a single byte from the stream.
        /// </summary>
        /// <returns>The integer representation of the byte.</returns>
        public override int ReadByte()
        {
            byte[] data = null;
            if (!_Kvpbase.ReadObjectRange(_Container, _ObjectKey, _Position, 1, out data))
            {
                throw new IOException("Unable to read data.");
            }

            _Position++;
            int ret = (int)data[0];
            return ret;
        }

        /// <summary>
        /// Seek to a specific position in the stream.
        /// </summary>
        /// <param name="offset">Offset from the specified origin.</param>
        /// <param name="origin">Seek origin, i.e. Begin, Current, or End.</param>
        /// <returns>Seek position.</returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            long tempPosition = _Position;

            switch (origin)
            {
                case SeekOrigin.Begin:
                    tempPosition = offset;
                    break;
                case SeekOrigin.Current:
                    tempPosition += offset;
                    break;
                case SeekOrigin.End:
                    tempPosition = Convert.ToInt64(_ObjectMetadata.ContentLength) + offset;
                    break;
            }

            if (tempPosition < 0) throw new ArgumentException("Resulting position would be less than zero.");
            _Position = tempPosition;
            return _Position;
        }

        /// <summary>
        /// Unsupported method.
        /// </summary> 
        public override void SetLength(long value)
        {
            _Length = value;
        }

        /// <summary>
        /// Unsupported method.
        /// </summary> 
        public override string ToString()
        {
            return base.ToString();
        }

        /// <summary>
        /// Write data to the stream.
        /// </summary>
        /// <param name="buffer">The byte array containing data to write.</param>
        /// <param name="offset">Offset within the buffer.</param>
        /// <param name="count">Number of bytes to write.</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (buffer == null || buffer.Length < 1) return;

            if (offset != 0 || count != buffer.Length)
            {
                // using only a subset of the original byte array
                byte[] temp = new byte[count];
                Buffer.BlockCopy(buffer, offset, temp, 0, count);
                buffer = new byte[count];
                Buffer.BlockCopy(temp, 0, buffer, 0, count);
            }

            if (!_Kvpbase.WriteObjectRange(_Container, _ObjectKey, _Position, buffer))
            {
                throw new IOException("Unable to write data.");
            }

            if (!GetObjectMetadata())
            {
                throw new IOException("Unable to read object metadata.");
            }

            _Position += count;
        }

        /// <summary>
        /// Write data asynchronously to the stream.
        /// </summary>
        /// <param name="buffer">The byte array containing data to write.</param>
        /// <param name="offset">Offset within the buffer.</param>
        /// <param name="count">Number of bytes to write.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Task.</returns>
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            if (offset > buffer.Length) throw new ArgumentOutOfRangeException(nameof(offset));
            if (offset + count > buffer.Length) throw new ArgumentOutOfRangeException(nameof(count));

            return Task.Run(() =>
            {
                if (offset != 0 || count != buffer.Length)
                {
                    // using only a subset of the original byte array
                    byte[] temp = new byte[count];
                    Buffer.BlockCopy(buffer, offset, temp, 0, count);
                    buffer = new byte[count];
                    Buffer.BlockCopy(temp, 0, buffer, 0, count);
                }

                if (!_Kvpbase.WriteObjectRange(_Container, _ObjectKey, _Position, buffer))
                {
                    throw new IOException("Unable to write data.");
                }

                if (!GetObjectMetadata())
                {
                    throw new IOException("Unable to read object metadata.");
                }

                _Position += count;

            }, cancellationToken);
        }

        /// <summary>
        /// Write a byte to the stream.
        /// </summary>
        /// <param name="value">The byte to write.</param>
        public override void WriteByte(byte value)
        {
            byte[] data = new byte[1];
            data[0] = value;

            if (!_Kvpbase.WriteObjectRange(_Container, _ObjectKey, _Position, data))
            {
                throw new IOException("Unable to write data.");
            }

            if (!GetObjectMetadata())
            {
                throw new IOException("Unable to read object metadata.");
            }

            _Position++;
        }

        /// <summary>
        /// Unsupported method.
        /// </summary> 
        [Obsolete]
        protected override WaitHandle CreateWaitHandle() => throw new NotImplementedException();

        /// <summary>
        /// Unsupported method.
        /// </summary> 
        protected override void Dispose(bool disposing)
        {
            if (_Disposed) return;

            // flush anything remaining
        }

        /// <summary>
        /// Unsupported method.
        /// </summary> 
        [Obsolete]
        protected override void ObjectInvariant() => throw new NotImplementedException();

        #endregion
    }
}
