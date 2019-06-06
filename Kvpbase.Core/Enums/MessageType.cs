using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace Kvpbase.Core
{
    /// <summary>
    /// Types of messages exchanged amongst nodes using the mesh network.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum MessageType
    { 
        [EnumMember(Value = "Console")]
        Console,  
        [EnumMember(Value = "ContainerExists")]
        ContainerExists,
        [EnumMember(Value = "ContainerEnumerate")]
        ContainerEnumerate,
        [EnumMember(Value = "ContainerList")]
        ContainerList,
        [EnumMember(Value = "ContainerDelete")]
        ContainerDelete,
        [EnumMember(Value = "ContainerCreate")]
        ContainerCreate,
        [EnumMember(Value = "ContainerUpdate")]
        ContainerUpdate,
        [EnumMember(Value = "ContainerClearAuditLog")]
        ContainerClearAuditLog,
        [EnumMember(Value = "ObjectExists")]
        ObjectExists,
        [EnumMember(Value = "ObjectMetadata")]
        ObjectMetadata,
        [EnumMember(Value = "ObjectRead")]
        ObjectRead,
        [EnumMember(Value = "ObjectDelete")]
        ObjectDelete,
        [EnumMember(Value = "ObjectCreate")]
        ObjectCreate,
        [EnumMember(Value = "ObjectRename")]
        ObjectRename,
        [EnumMember(Value = "ObjectWriteRange")]
        ObjectWriteRange,
        [EnumMember(Value = "ObjectWriteTags")]
        ObjectWriteTags
    }
}
