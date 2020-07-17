﻿using System;
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
            public string UserGuid { get; set; }

            /// <summary>
            /// The name of the container.
            /// </summary>
            public string ContainerName { get; set; }

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
            /// Indicates if the request is to write object or container keys.
            /// </summary>
            public bool Keys { get; set; }

            /// <summary>
            /// Indicates if the request is a search query.
            /// </summary>
            public bool Search { get; set; }

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

                ret.UserGuid = "null";
                if (req.RawUrlEntries.Count >= 1) ret.UserGuid = WebUtility.UrlDecode(req.RawUrlEntries[0]);

                if (req.RawUrlEntries.Count > 1) ret.ContainerName = WebUtility.UrlDecode(req.RawUrlEntries[1]);
                if (req.RawUrlEntries.Count > 2)
                {
                    string rawUrl = req.RawUrlWithoutQuery;
                    while (rawUrl.StartsWith("/")) rawUrl = rawUrl.Substring(1);
                    string[] vals = rawUrl.Split(new[] { '/' }, 3);
                    ret.ObjectKey = WebUtility.UrlDecode(vals[2]);
                }

                if (req.QuerystringEntries.ContainsKey("auditlog")) ret.AuditLog = true; 
                if (req.QuerystringEntries.ContainsKey("metadata")) ret.Metadata = true;
                if (req.QuerystringEntries.ContainsKey("keys")) ret.Keys = true;
                if (req.QuerystringEntries.ContainsKey("search")) ret.Search = true;
                if (req.QuerystringEntries.ContainsKey("reqmetadata")) ret.RequestMetadata = true;
                if (req.QuerystringEntries.ContainsKey("config")) ret.Config = true; 
                if (req.QuerystringEntries.ContainsKey("html")) ret.Html = true;

                if (req.QuerystringEntries.ContainsKey("auditkey"))
                    ret.AuditKey = req.QuerystringEntries["auditkey"];

                if (req.QuerystringEntries.ContainsKey("action"))
                    ret.Action = req.QuerystringEntries["action"];

                if (req.QuerystringEntries.ContainsKey("index"))
                {
                    if (Int32.TryParse(req.QuerystringEntries["index"], out testInt))
                    {
                        if (testInt >= 0) ret.Index = testInt;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("count"))
                {
                    if (Int32.TryParse(req.QuerystringEntries["count"], out testInt))
                    {
                        if (testInt >= 0) ret.Count = testInt;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("rename"))
                    ret.Rename = req.QuerystringEntries["rename"];

                if (req.QuerystringEntries.ContainsKey("createdbefore"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["createdbefore"], out testTimestamp))
                    {
                        ret.CreatedBefore = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("createdafter"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["createdafter"], out testTimestamp))
                    {
                        ret.CreatedAfter = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("updatedbefore"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["updatedbefore"], out testTimestamp))
                    {
                        ret.UpdatedBefore = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("updatedafter"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["updatedafter"], out testTimestamp))
                    {
                        ret.UpdatedAfter = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("accessedbefore"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["_accessedbefore"], out testTimestamp))
                    {
                        ret.LastAccessBefore = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("accessedafter"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["_accessedafter"], out testTimestamp))
                    {
                        ret.LastAccessAfter = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("prefix"))
                    ret.Prefix = req.QuerystringEntries["prefix"];

                if (req.QuerystringEntries.ContainsKey("md5"))
                    ret.Md5 = req.QuerystringEntries["md5"];

                if (req.QuerystringEntries.ContainsKey("orderby"))
                    ret.OrderBy = req.QuerystringEntries["orderby"];

                if (req.QuerystringEntries.ContainsKey("contenttype"))
                    ret.ContentType = req.QuerystringEntries["contenttype"];

                if (req.QuerystringEntries.ContainsKey("tags"))
                    ret.Tags = req.QuerystringEntries["tags"];

                if (req.QuerystringEntries.ContainsKey("sizemin"))
                {
                    if (Int64.TryParse(req.QuerystringEntries["sizemin"], out testLong))
                    {
                        ret.SizeMin = testLong;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("sizemax"))
                {
                    if (Int64.TryParse(req.QuerystringEntries["sizemax"], out testLong))
                    {
                        ret.SizeMax = testLong;
                    }
                }
                 
                return ret;
            } 
        } 
    }
}
