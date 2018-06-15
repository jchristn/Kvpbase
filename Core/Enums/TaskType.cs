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
    /// Types of tasks being processed in the background. 
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum TaskType
    {
        [EnumMember(Value = "Message")]
        Message 
    }
}
