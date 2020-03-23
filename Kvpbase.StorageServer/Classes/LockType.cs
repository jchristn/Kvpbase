using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace Kvpbase.StorageServer.Classes
{
    /// <summary>
    /// Entry types for URL locks.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum LockType
    {
        /// <summary>
        /// Read lock.
        /// </summary>
        [EnumMember(Value = "Read")]
        Read,
        /// <summary>
        /// Write lock.
        /// </summary>
        [EnumMember(Value = "Write")]
        Write 
    }
}
