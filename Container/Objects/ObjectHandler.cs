using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kvpbase.Container
{
    /// <summary>
    /// Object handler template.
    /// </summary>
    internal abstract class ObjectHandler
    {
        /// <summary>
        /// Check if an object exists.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <returns>True if exists.</returns>
        public abstract bool Exists(string fullFilename);

        /// <summary>
        /// Write an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="data">The object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool Write(string fullFilename, byte[] data, out ErrorCode error);

        /// <summary>
        /// Write an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.  The file will be re-read in its entirety to calculate the MD5.</param>
        /// <param name="contentLength">The length of the data.</param>
        /// <param name="stream">The stream containing the data.</param>
        /// <param name="md5">The MD5 of the data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool Write(string fullFilename, long contentLength, Stream stream, out string md5, out ErrorCode error);

        /// <summary>
        /// Write bytes to a specific position in an existing object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="position">The byte position to which data should be written.</param>
        /// <param name="data">The data to write.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool WriteRange(string fullFilename, long position, byte[] data, out ErrorCode error);

        /// <summary>
        /// Write bytes to a specific position in an existing object.  The file will be re-read in its entirety to calculate the MD5.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="position">The byte position to which data should be written.</param>
        /// <param name="contentLength">The content length of the bytes to write.</param>
        /// <param name="stream">The stream containing the data.</param>
        /// <param name="md5">The MD5 of the updated object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool WriteRange(string fullFilename, long position, long contentLength, Stream stream, out string md5, out ErrorCode error);

        /// <summary>
        /// Read an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="data">The object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool Read(string fullFilename, out byte[] data, out ErrorCode error);

        /// <summary>
        /// Read an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="contentLength">The content length of the object.</param>
        /// <param name="stream">The stream containing the object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool Read(string fullFilename, out long contentLength, out Stream stream, out ErrorCode error);

        /// <summary>
        /// Read bytes from a specific range in an existing object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="position">The byte position from which data should be read.</param>
        /// <param name="count">The number of bytes to read.</param>
        /// <param name="data">The object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool ReadRange(string fullFilename, long position, int count, out byte[] data, out ErrorCode error);

        /// <summary>
        /// Read bytes from a specific range in an existing object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="position">The byte position from which data should be read.</param>
        /// <param name="count">The number of bytes to read.</param>
        /// <param name="stream">The stream containing the object data.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool ReadRange(string fullFilename, long position, int count, out Stream stream, out ErrorCode error);

        /// <summary>
        /// Delete an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool Delete(string fullFilename, out ErrorCode error);

        /// <summary>
        /// Rename an object.
        /// </summary>
        /// <param name="original">The full path and filename of the object.</param>
        /// <param name="updated">The desired full path and filename of the object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool Rename(string original, string updated, out ErrorCode error);

        /// <summary>
        /// Retrieve the size of an object.
        /// </summary>
        /// <param name="fullFilename">The full path and filename of the object.</param>
        /// <param name="size">The size of the object.</param>
        /// <param name="error">Error code.</param>
        /// <returns>True if successful.</returns>
        public abstract bool GetObjectSize(string fullFilename, out long size, out ErrorCode error); 
    }
}
