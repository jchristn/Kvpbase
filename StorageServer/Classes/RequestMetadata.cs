using System;
using System.Collections.Generic;
using SyslogLogging;
using WatsonWebserver;
using RestWrapper;

namespace Kvpbase
{
    public class RequestMetadata
    {
        public HttpRequest CurrentHttpRequest { get; set; }
        public UserMaster CurrentUserMaster { get; set; }
        public ApiKey CurrentApiKey { get; set; }
        public ApiKeyPermission CurrentApiKeyPermission { get; set; }
        public Node CurrentNode { get; set; }
        public Obj CurrentObj { get; set; }

        public object FirstResponseLock { get; set; }
        public RestResponse FirstResponse { get; set; }
        public string FirstResponseUrl { get; set; }
    }
}
