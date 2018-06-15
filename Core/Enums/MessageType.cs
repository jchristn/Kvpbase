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
    /// Types of messages exchanged amongst nodes using the mesh network.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum MessageType
    {
        [EnumMember(Value = "Hello")]
        Hello,
        [EnumMember(Value = "Console")]
        Console,
        [EnumMember(Value = "ContainerExists")]
        ContainerExists,
        [EnumMember(Value = "ContainerEnumerate")]
        ContainerEnumerate,
        [EnumMember(Value = "ContainerList")]
        ContainerList,
        [EnumMember(Value = "ReplicationContainerDelete")]
        ReplicationContainerDelete,
        [EnumMember(Value = "ReplicationContainerCreate")]
        ReplicationContainerCreate,
        [EnumMember(Value = "ReplicationContainerUpdate")]
        ReplicationContainerUpdate,
        [EnumMember(Value = "ReplicationContainerClearAuditLog")]
        ReplicationContainerClearAuditLog,
        [EnumMember(Value = "ObjectExists")]
        ObjectExists,
        [EnumMember(Value = "ObjectMetadata")]
        ObjectMetadata,
        [EnumMember(Value = "ObjectRead")]
        ObjectRead,
        [EnumMember(Value = "ReplicationObjectDelete")]
        ReplicationObjectDelete,
        [EnumMember(Value = "ReplicationObjectCreate")]
        ReplicationObjectCreate,
        [EnumMember(Value = "ReplicationObjectRename")]
        ReplicationObjectRename,
        [EnumMember(Value = "ReplicationObjectWriteRange")]
        ReplicationObjectWriteRange
    }
}
