using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kvpbase.StorageServer.Classes
{ 
    internal abstract class StorageDriver
    { 
        internal abstract bool Exists(string fullFilename); 
        internal abstract bool Write(string fullFilename, long contentLength, Stream stream, out string md5, out ErrorCode error); 
        internal abstract bool WriteRange(string fullFilename, long position, long contentLength, Stream stream, out string md5, out ErrorCode error); 
        internal abstract bool Read(string fullFilename, out long contentLength, out Stream stream, out ErrorCode error); 
        internal abstract bool ReadRange(string fullFilename, long position, int count, out Stream stream, out ErrorCode error); 
        internal abstract bool Delete(string fullFilename, out ErrorCode error); 
        internal abstract bool Rename(string original, string updated, out ErrorCode error); 
        internal abstract bool GetObjectSize(string fullFilename, out long size, out ErrorCode error);
    }
}
