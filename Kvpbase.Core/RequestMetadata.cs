using System;
using System.Collections.Generic;
using System.Text; 
using WatsonWebserver; 

namespace Kvpbase.Core
{
    /// <summary>
    /// Metadata for an incoming HTTP API request.
    /// </summary>
    public class RequestMetadata
    {
        #region Public-Members
         
        /// <summary>
        /// The HttpRequest object from the web server.
        /// </summary>
        public HttpRequest Http { get; set; }

        /// <summary>
        /// The user object.
        /// </summary>
        public UserMaster User { get; set; }

        /// <summary>
        /// The API key object.
        /// </summary>
        public ApiKey Key { get; set; }

        /// <summary>
        /// The permission object associated with the API key.
        /// </summary>
        public ApiKeyPermission Perm { get; set; }

        /// <summary>
        /// The node that received the request.
        /// </summary>
        public Node Node { get; set; }

        /// <summary>
        /// Parameters as found in querystring key-value pairs and the URL itself.
        /// </summary>
        public Parameters Params { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public RequestMetadata()
        {

        }
        
        public static RequestMetadata Default()
        {
            RequestMetadata md = new RequestMetadata();

            md.Http = new HttpRequest();
            md.Http.TimestampUtc = DateTime.Now.ToUniversalTime();
            md.Http.Data = null;
            md.Http.Headers = new Dictionary<string, string>();
            md.Http.ContentType = "application/json";
            md.Http.ContentLength = 0;
            md.Http.Useragent = null;
            md.Http.QuerystringEntries = new Dictionary<string, string>();
            md.Http.RawUrlEntries = new List<string>();
            md.Http.RawUrlWithoutQuery = "/";
            md.Http.RawUrlWithQuery = "/";
            md.Http.Querystring = null;
            md.Http.Method = HttpMethod.GET;
            md.Http.ThreadId = 0;
            md.Http.FullUrl = "/";
            md.Http.SourceIp = "127.0.0.1";
            md.Http.SourcePort = 0;
            md.Http.DestIp = "127.0.0.1";
            md.Http.DestPort = 0;
            md.Http.DestHostPort = 0;
            md.Http.DestHostname = "127.0.0.1";
            md.Http.Keepalive = false;

            md.User = null;
            md.Key = null;
            md.Perm = null;
            md.Node = null;
            md.Params = new Parameters();

            return md;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Return a sanitized RequestMetadata object, which can then be serialized.
        /// Certain properties within HttpRequest cannot be serialized, so objects must be sanitized first.
        /// </summary>
        /// <returns>Serializable version of the object.</returns>
        public RequestMetadata Sanitized()
        {
            RequestMetadata md = new RequestMetadata(); 
            md.User = User;
            md.Key = Key;
            md.Perm = Perm;
            md.Node = Node;

            md.Params = new RequestMetadata.Parameters();
            md.Params.UserGuid = Params.UserGuid;
            md.Params.Container = Params.Container;
            md.Params.ObjectKey = Params.ObjectKey;
            md.Params.AuditKey = Params.AuditKey;
            md.Params.AuditLog = Params.AuditLog;
            md.Params.Metadata = Params.Metadata;
            md.Params.Action = Params.Action;
            md.Params.Index = Params.Index;
            md.Params.Count = Params.Count;
            md.Params.Rename = Params.Rename;
            md.Params.CreatedBefore = Params.CreatedBefore;
            md.Params.CreatedAfter = Params.CreatedAfter;
            md.Params.UpdatedBefore = Params.UpdatedBefore;
            md.Params.UpdatedAfter = Params.UpdatedAfter;
            md.Params.LastAccessBefore = Params.LastAccessBefore;
            md.Params.LastAccessAfter = Params.LastAccessAfter;
            md.Params.Md5 = Params.Md5;
            md.Params.ContentType = Params.ContentType;
            md.Params.Tags = Params.Tags;
            md.Params.SizeMin = Params.SizeMin;
            md.Params.SizeMax = Params.SizeMax;
            md.Params.OrderBy = Params.OrderBy;

            md.Http = null;
            if (Http != null)
            {
                md.Http = new HttpRequest();
                md.Http.TimestampUtc = Http.TimestampUtc;
                md.Http.Data = Http.Data;
                md.Http.Headers = Http.Headers;
                md.Http.ContentType = Http.ContentType;
                md.Http.ContentLength = Http.ContentLength;
                md.Http.Useragent = Http.Useragent;
                md.Http.QuerystringEntries = Http.QuerystringEntries;
                md.Http.RawUrlEntries = Http.RawUrlEntries;
                md.Http.RawUrlWithoutQuery = Http.RawUrlWithoutQuery;
                md.Http.RawUrlWithQuery = Http.RawUrlWithQuery;
                md.Http.Querystring = Http.Querystring;
                md.Http.Method = Http.Method;
                md.Http.ThreadId = Http.ThreadId;
                md.Http.FullUrl = Http.FullUrl;
                md.Http.SourceIp = Http.SourceIp;
                md.Http.SourcePort = Http.SourcePort;
                md.Http.DestIp = Http.DestIp;
                md.Http.DestPort = Http.DestPort;
                md.Http.DestHostPort = Http.DestHostPort;
                md.Http.DestHostname = Http.DestHostname;
                md.Http.Keepalive = Http.Keepalive; 
            }

            return md;
        }

        /// <summary>
        /// Returns byte array containing the JSON-serialized representation of the object, useful for transmission over the peer-to-peer mesh network.
        /// </summary>
        /// <returns>Byte array.</returns>
        public byte[] ToBytes()
        {
            return Encoding.UTF8.GetBytes(Common.SerializeJson(Sanitized(), false)); 
        }

        #endregion

        #region Private-Methods

        #endregion

        #region Public-Embedded-Classes

        /// <summary>
        /// Request parameters found in querystring key-value pairs and the URL itself.
        /// </summary>
        public class Parameters
        {
            /// <summary>
            /// The user GUID.
            /// </summary>
            public string UserGuid { get; set; }

            /// <summary>
            /// The name of the container.
            /// </summary>
            public string Container { get; set; }

            /// <summary>
            /// The key of the object.
            /// </summary>
            public string ObjectKey { get; set; }

            /// <summary>
            /// The key for which to search when querying audit logs.
            /// </summary>
            public string AuditKey { get; set; }

            /// <summary>
            /// The action for which to search when querying audit logs.
            /// </summary>
            public string Action { get; set; }

            /// <summary>
            /// The index, or, starting position.
            /// </summary>
            public Int64? Index = null;

            /// <summary>
            /// The number of records to retrieve, or number of bytes.
            /// </summary>
            public Int64? Count = null;

            /// <summary>
            /// The name to which an object should be renamed.
            /// </summary>
            public string Rename { get; set; }

            /// <summary>
            /// Indicates if configuration-related information is desired.
            /// </summary>
            public bool Config { get; set; }

            /// <summary>
            /// Indicates if stats-related information is desired.
            /// </summary>
            public bool Stats { get; set; }

            /// <summary>
            /// Indicates if HTML output is desired.
            /// </summary>
            public bool Html { get; set; }

            /// <summary>
            /// Indicates if the request is an audit log query.
            /// </summary>
            public bool AuditLog { get; set; }

            /// <summary>
            /// Indicates if the request is a metadata query.
            /// </summary>
            public bool Metadata { get; set; }

            /// <summary>
            /// Indicates if the request is attempting to retrieve HTTP and internal metadata.
            /// </summary>
            public bool RequestMetadata { get; set; }

            /// <summary>
            /// For queries, the time before which the resource must have been created.
            /// </summary>
            public DateTime? CreatedBefore { get; set; }

            /// <summary>
            /// For queries, the time after which the resource must have been created.
            /// </summary>
            public DateTime? CreatedAfter { get; set; }

            /// <summary>
            /// For queries, the time before which the resource must have been updated.
            /// </summary>
            public DateTime? UpdatedBefore { get; set; }

            /// <summary>
            /// For queries, the time after which the resource must have been updated.
            /// </summary>
            public DateTime? UpdatedAfter { get; set; }

            /// <summary>
            /// For queries, the time before which the resource must have been last accessed.
            /// </summary>
            public DateTime? LastAccessBefore { get; set; }

            /// <summary>
            /// For queries, the time after which the resource must have been last accessed.
            /// </summary>
            public DateTime? LastAccessAfter { get; set; }

            /// <summary>
            /// For queries, the prefix for the object key.
            /// </summary>
            public string Prefix { get; set; }

            /// <summary>
            /// For queries, the MD5 that must match with the resource MD5.
            /// </summary>
            public string Md5 { get; set; }

            /// <summary>
            /// For queries, the content type that must match with the resource content type.
            /// </summary>
            public string ContentType { get; set; }

            /// <summary>
            /// The tags associated with an object.
            /// </summary>
            public string Tags { get; set; }

            /// <summary>
            /// For queries, the minimum size allowed.
            /// </summary>
            public long? SizeMin { get; set; }

            /// <summary>
            /// For queries, the maximum size allowed.
            /// </summary>
            public long? SizeMax { get; set; }

            /// <summary>
            /// For queries, the SQL ordering that should be applied to the result.
            /// i.e. 'ORDER BY LastUpdateUtc ASC'.
            /// </summary>
            public string OrderBy { get; set; }

            /// <summary>
            /// Instantiates the object.
            /// </summary>
            public Parameters()
            {
                UserGuid = null;
                Container = null;
                ObjectKey = null;
                AuditKey = null;
                Action = null;
                Index = null;
                Count = null;
                Rename = null;
                Config = false;
                Stats = false;
                Html = false;
                AuditLog = false;
                Metadata = false;
                RequestMetadata = false;
                CreatedBefore = null;
                CreatedAfter = null;
                UpdatedBefore = null;
                UpdatedAfter = null;
                LastAccessBefore = null;
                LastAccessAfter = null;
                Prefix = null;
                Md5 = null;
                ContentType = null;
                Tags = null;
                SizeMin = null;
                SizeMax = null;
                OrderBy = null;
            }
        }

        #endregion
    }
}
