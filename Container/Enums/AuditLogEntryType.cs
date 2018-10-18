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
    /// Entry types for the container audit log.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum AuditLogEntryType
    {
        [EnumMember(Value = "Enumerate")]
        Enumerate,
        [EnumMember(Value = "Read")]
        Read,
        [EnumMember(Value = "ReadRange")]
        ReadRange,
        [EnumMember(Value = "Write")]
        Write,
        [EnumMember(Value = "WriteRange")]
        WriteRange,
        [EnumMember(Value = "WriteTags")]
        WriteTags,
        [EnumMember(Value = "Delete")]
        Delete,
        [EnumMember(Value = "Rename")]
        Rename,
        [EnumMember(Value = "Exists")]
        Exists,
        [EnumMember(Value = "Configuration")]
        Configuration
    }
}
