using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace Kvpbase
{
    /// <summary>
    /// Types of redirection modes used when requests are received on nodes that cannot fulfill the request. 
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum RedirectMode
    {
        [EnumMember(Value = "MovedPermanently")]
        MovedPermanently,
        [EnumMember(Value = "Found")]
        Found,
        [EnumMember(Value = "TemporaryRedirect")]
        TemporaryRedirect,
        [EnumMember(Value = "PermanentRedirect")]
        PermanentRedirect
    }
}
