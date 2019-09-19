using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace Kvpbase.Classes
{ 
    [JsonConverter(typeof(StringEnumConverter))]
    public enum AuthResult
    {
        [EnumMember(Value = "None")]
        None,
        [EnumMember(Value = "NoMaterialSupplied")]
        NoMaterialSupplied,
        [EnumMember(Value = "ApiKeyNotFound")]
        ApiKeyNotFound,
        [EnumMember(Value = "UserNotFound")]
        UserNotFound,
        [EnumMember(Value = "ApiKeyInactive")]
        ApiKeyInactive,
        [EnumMember(Value = "UserInactive")]
        UserInactive,
        [EnumMember(Value = "InvalidCredentials")]
        InvalidCredentials,
        [EnumMember(Value = "Success")]
        Success
    }
}
