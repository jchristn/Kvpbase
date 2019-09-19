using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace Kvpbase.Containers
{ 
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ErrorCode
    {
        [EnumMember(Value = "AlreadyExists")]
        AlreadyExists,
        [EnumMember(Value = "Success")]
        Success,
        [EnumMember(Value = "Created")]
        Created,
        [EnumMember(Value = "None")]
        None,
        [EnumMember(Value = "ServerError")]
        ServerError, 
        [EnumMember(Value = "NotFound")]
        NotFound, 
        [EnumMember(Value = "IOError")]
        IOError,
        [EnumMember(Value = "StreamError")]
        StreamError,
        [EnumMember(Value = "PermissionsError")]
        PermissionsError,
        [EnumMember(Value = "DiskFull")]
        DiskFull,
        [EnumMember(Value = "Locked")]
        Locked,
        [EnumMember(Value = "OutOfRange")]
        OutOfRange
    }
}
