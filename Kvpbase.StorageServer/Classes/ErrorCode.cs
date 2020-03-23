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
    /// Error code.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ErrorCode
    {
        /// <summary>
        /// Already exists.
        /// </summary>
        [EnumMember(Value = "AlreadyExists")]
        AlreadyExists,
        /// <summary>
        /// Success.
        /// </summary>
        [EnumMember(Value = "Success")]
        Success,
        /// <summary>
        /// Created.
        /// </summary>
        [EnumMember(Value = "Created")]
        Created,
        /// <summary>
        /// None.
        /// </summary>
        [EnumMember(Value = "None")]
        None,
        /// <summary>
        /// Server error.
        /// </summary>
        [EnumMember(Value = "ServerError")]
        ServerError, 
        /// <summary>
        /// Not found.
        /// </summary>
        [EnumMember(Value = "NotFound")]
        NotFound, 
        /// <summary>
        /// I/O error.
        /// </summary>
        [EnumMember(Value = "IOError")]
        IOError,
        /// <summary>
        /// Stream error.
        /// </summary>
        [EnumMember(Value = "StreamError")]
        StreamError,
        /// <summary>
        /// Permissions error.
        /// </summary>
        [EnumMember(Value = "PermissionsError")]
        PermissionsError,
        /// <summary>
        /// Disk full.
        /// </summary>
        [EnumMember(Value = "DiskFull")]
        DiskFull,
        /// <summary>
        /// Locked.
        /// </summary>
        [EnumMember(Value = "Locked")]
        Locked,
        /// <summary>
        /// Out of range.
        /// </summary>
        [EnumMember(Value = "OutOfRange")]
        OutOfRange
    }
}
