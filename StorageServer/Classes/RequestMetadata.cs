using System;
using System.Collections.Generic;
using SyslogLogging;
using WatsonWebserver;
using RestWrapper;

namespace Kvpbase
{
    public class RequestMetadata
    {
        #region Public-Members

        public HttpRequest CurrHttpReq { get; set; }
        public UserMaster CurrUser { get; set; }
        public ApiKey CurrApiKey { get; set; }
        public ApiKeyPermission CurrPerm { get; set; }
        public Node CurrNode { get; set; }
        public Obj CurrObj { get; set; }

        public object FirstRespLock { get; set; }
        public RestResponse FirstResponse { get; set; }
        public string FirstResponseUrl { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods
        
        #endregion
    }
}
