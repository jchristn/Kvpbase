using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text; 
using WatsonWebserver;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer.Classes
{
    /// <summary>
    /// Metadata for an incoming HTTP API request.
    /// </summary>
    public class RequestMetadata
    { 
        /// <summary>
        /// The HttpContext object from the web server.
        /// </summary>
        public HttpContext Http { get; set; }

        /// <summary>
        /// The user object.
        /// </summary>
        public UserMaster User { get; set; }

        /// <summary>
        /// The API key object.
        /// </summary>
        public ApiKey Key { get; set; }

        /// <summary>
        /// The permission object associated with the requestor.
        /// </summary>
        public Permission Perm { get; set; }
         
        /// <summary>
        /// Parameters as found in querystring key-value pairs and the URL itself.
        /// </summary>
        public Parameters Params { get; set; }
         
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public RequestMetadata()
        {

        }
          
        /// <summary>
        /// Request parameters found in querystring key-value pairs and the URL itself.
        /// </summary>
        public class Parameters
        {
            /// <summary>
            /// The user GUID.
            /// </summary>
            public string UserGUID = null;

            /// <summary>
            /// The name of the container.
            /// </summary>
            public string ContainerName = null;

            /// <summary>
            /// The key of the object.
            /// </summary>
            public string ObjectKey = null;

            /// <summary>
            /// The key for which to search when querying audit logs.
            /// </summary>
            public string AuditKey = null;

            /// <summary>
            /// The action for which to search when querying audit logs.
            /// </summary>
            public string Action = null;
             
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
            public string Rename = null;

            /// <summary>
            /// Indicates if configuration-related information is desired.
            /// </summary>
            public bool Config = false;

            /// <summary>
            /// Indicates if HTML output is desired.
            /// </summary>
            public bool Html = false;

            /// <summary>
            /// Indicates if the request is an audit log query.
            /// </summary>
            public bool AuditLog = false;

            /// <summary>
            /// Indicates if the request is a metadata query.
            /// </summary>
            public bool Metadata = false;

            /// <summary>
            /// Indicates if the request is to write object or container keys.
            /// </summary>
            public bool Keys = false;

            /// <summary>
            /// Indicates if the request is a search query.
            /// </summary>
            public bool Search = false;

            /// <summary>
            /// Indicates if the request is attempting to retrieve HTTP and internal metadata.
            /// </summary>
            public bool RequestMetadata = false;

            /// <summary>
            /// For queries, the time before which the resource must have been created.
            /// </summary>
            public DateTime? CreatedBefore = null;

            /// <summary>
            /// For queries, the time after which the resource must have been created.
            /// </summary>
            public DateTime? CreatedAfter = null;

            /// <summary>
            /// For queries, the time before which the resource must have been updated.
            /// </summary>
            public DateTime? UpdatedBefore = null;

            /// <summary>
            /// For queries, the time after which the resource must have been updated.
            /// </summary>
            public DateTime? UpdatedAfter = null;

            /// <summary>
            /// For queries, the time before which the resource must have been last accessed.
            /// </summary>
            public DateTime? LastAccessBefore = null;

            /// <summary>
            /// For queries, the time after which the resource must have been last accessed.
            /// </summary>
            public DateTime? LastAccessAfter = null;

            /// <summary>
            /// For lock creation requests, the timestamp by when the lock must expire.
            /// </summary>
            public DateTime? ExpirationUtc = null;

            /// <summary>
            /// For queries, the prefix for the object key.
            /// </summary>
            public string Prefix = null;

            /// <summary>
            /// For queries, the MD5 that must match with the resource MD5.
            /// </summary>
            public string Md5 = null;

            /// <summary>
            /// For queries, the content type that must match with the resource content type.
            /// </summary>
            public string ContentType = null;

            /// <summary>
            /// The tags associated with an object.
            /// </summary>
            public string Tags = null;

            /// <summary>
            /// For queries, the minimum size allowed.
            /// </summary>
            public long? SizeMin = null;

            /// <summary>
            /// For queries, the maximum size allowed.
            /// </summary>
            public long? SizeMax = null;

            /// <summary>
            /// For queries, the SQL ordering that should be applied to the result.
            /// i.e. 'ORDER BY LastUpdateUtc ASC'.
            /// </summary>
            public string OrderBy = null;

            /// <summary>
            /// Indicates that the request is to apply a write lock on a given object.
            /// </summary>
            public bool WriteLock = false;

            /// <summary>
            /// Indicates that the request is to apply a read lock on a given object.
            /// </summary>
            public bool ReadLock = false;

            /// <summary>
            /// Specifies the lock GUID to remove.
            /// </summary>
            public string LockGUID = null;

            /// <summary>
            /// Instantiates the object.
            /// </summary>
            public Parameters()
            { 
            }

            /// <summary>
            /// Instantiates the object.
            /// </summary>
            /// <param name="req">HttpRequest.</param>
            /// <returns>Parameters.</returns>
            public static Parameters FromHttpRequest(HttpRequest req)
            {
                if (req == null) throw new ArgumentNullException(nameof(req));

                Parameters ret = new Parameters();

                DateTime testTimestamp;
                int testInt;
                long testLong = 0;

                ret.UserGUID = "null";
                if (req.Url.Elements.Length >= 1) ret.UserGUID = WebUtility.UrlDecode(req.Url.Elements[0]);

                if (req.Url.Elements.Length > 1) ret.ContainerName = WebUtility.UrlDecode(req.Url.Elements[1]);
                if (req.Url.Elements.Length > 2)
                {
                    string rawUrl = req.Url.RawWithoutQuery;
                    while (rawUrl.StartsWith("/")) rawUrl = rawUrl.Substring(1);
                    string[] vals = rawUrl.Split(new[] { '/' }, 3);
                    ret.ObjectKey = WebUtility.UrlDecode(vals[2]);
                }

                if (req.Query.Elements.ContainsKey("auditlog")) ret.AuditLog = true; 
                if (req.Query.Elements.ContainsKey("metadata")) ret.Metadata = true;
                if (req.Query.Elements.ContainsKey("keys")) ret.Keys = true;
                if (req.Query.Elements.ContainsKey("search")) ret.Search = true;
                if (req.Query.Elements.ContainsKey("reqmetadata")) ret.RequestMetadata = true;
                if (req.Query.Elements.ContainsKey("config")) ret.Config = true;
                if (req.Query.Elements.ContainsKey("html")) ret.Html = true;
                if (req.Query.Elements.ContainsKey("writelock")) ret.WriteLock = true;
                if (req.Query.Elements.ContainsKey("readlock")) ret.ReadLock = true;


                if (req.Query.Elements.ContainsKey("auditkey"))
                    ret.AuditKey = req.Query.Elements["auditkey"];

                if (req.Query.Elements.ContainsKey("action"))
                    ret.Action = req.Query.Elements["action"];

                if (req.Query.Elements.ContainsKey("index"))
                {
                    if (Int32.TryParse(req.Query.Elements["index"], out testInt))
                    {
                        if (testInt >= 0) ret.Index = testInt;
                    }
                }

                if (req.Query.Elements.ContainsKey("count"))
                {
                    if (Int32.TryParse(req.Query.Elements["count"], out testInt))
                    {
                        if (testInt >= 0) ret.Count = testInt;
                    }
                }

                if (req.Query.Elements.ContainsKey("rename"))
                    ret.Rename = req.Query.Elements["rename"];

                if (req.Query.Elements.ContainsKey("createdbefore"))
                {
                    if (DateTime.TryParse(req.Query.Elements["createdbefore"], out testTimestamp))
                    {
                        ret.CreatedBefore = testTimestamp;
                    }
                }

                if (req.Query.Elements.ContainsKey("createdafter"))
                {
                    if (DateTime.TryParse(req.Query.Elements["createdafter"], out testTimestamp))
                    {
                        ret.CreatedAfter = testTimestamp;
                    }
                }

                if (req.Query.Elements.ContainsKey("updatedbefore"))
                {
                    if (DateTime.TryParse(req.Query.Elements["updatedbefore"], out testTimestamp))
                    {
                        ret.UpdatedBefore = testTimestamp;
                    }
                }

                if (req.Query.Elements.ContainsKey("updatedafter"))
                {
                    if (DateTime.TryParse(req.Query.Elements["updatedafter"], out testTimestamp))
                    {
                        ret.UpdatedAfter = testTimestamp;
                    }
                }

                if (req.Query.Elements.ContainsKey("accessedbefore"))
                {
                    if (DateTime.TryParse(req.Query.Elements["accessedbefore"], out testTimestamp))
                    {
                        ret.LastAccessBefore = testTimestamp;
                    }
                }

                if (req.Query.Elements.ContainsKey("accessedafter"))
                {
                    if (DateTime.TryParse(req.Query.Elements["accessedafter"], out testTimestamp))
                    {
                        ret.LastAccessAfter = testTimestamp;
                    }
                }

                if (req.Query.Elements.ContainsKey("expire"))
                {
                    if (DateTime.TryParse(req.Query.Elements["expire"], out testTimestamp))
                    {
                        ret.ExpirationUtc = testTimestamp;
                    }
                }

                if (req.Query.Elements.ContainsKey("prefix"))
                    ret.Prefix = req.Query.Elements["prefix"];

                if (req.Query.Elements.ContainsKey("md5"))
                    ret.Md5 = req.Query.Elements["md5"];

                if (req.Query.Elements.ContainsKey("orderby"))
                    ret.OrderBy = req.Query.Elements["orderby"];

                if (req.Query.Elements.ContainsKey("contenttype"))
                    ret.ContentType = req.Query.Elements["contenttype"];

                if (req.Query.Elements.ContainsKey("tags"))
                    ret.Tags = req.Query.Elements["tags"];

                if (req.Query.Elements.ContainsKey("lockguid"))
                    ret.LockGUID = req.Query.Elements["lockguid"];

                if (req.Query.Elements.ContainsKey("sizemin"))
                {
                    if (Int64.TryParse(req.Query.Elements["sizemin"], out testLong))
                    {
                        ret.SizeMin = testLong;
                    }
                }

                if (req.Query.Elements.ContainsKey("sizemax"))
                {
                    if (Int64.TryParse(req.Query.Elements["sizemax"], out testLong))
                    {
                        ret.SizeMax = testLong;
                    }
                }
                 
                return ret;
            } 
        } 
    }
}
