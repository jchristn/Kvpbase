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
    /// The type of object handler.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ObjectHandlerType
    {
        [EnumMember(Value = "Disk")]
        Disk
    }
}
