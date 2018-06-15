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
    /// Replication modes that can be specified when creating or updating a container.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ReplicationMode
    {
        [EnumMember(Value = "Async")]
        Async,
        [EnumMember(Value = "Sync")]
        Sync,
        [EnumMember(Value = "None")]
        None
    }
}
