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
    /// Authentication result.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum AuthResult
    {
        /// <summary>
        /// No result.
        /// </summary>
        [EnumMember(Value = "None")]
        None,
        /// <summary>
        /// No authentication material supplied.
        /// </summary>
        [EnumMember(Value = "NoMaterialSupplied")]
        NoMaterialSupplied,
        /// <summary>
        /// API key not found.
        /// </summary>
        [EnumMember(Value = "ApiKeyNotFound")]
        ApiKeyNotFound,
        /// <summary>
        /// User not found.
        /// </summary>
        [EnumMember(Value = "UserNotFound")]
        UserNotFound,
        /// <summary>
        /// API key inactive.
        /// </summary>
        [EnumMember(Value = "ApiKeyInactive")]
        ApiKeyInactive,
        /// <summary>
        /// User inactive.
        /// </summary>
        [EnumMember(Value = "UserInactive")]
        UserInactive,
        /// <summary>
        /// Invalid credentials.
        /// </summary>
        [EnumMember(Value = "InvalidCredentials")]
        InvalidCredentials,
        /// <summary>
        /// Success.
        /// </summary>
        [EnumMember(Value = "Success")]
        Success
    }
}
