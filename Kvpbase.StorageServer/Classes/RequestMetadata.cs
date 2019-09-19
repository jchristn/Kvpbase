using System;
using System.Collections.Generic;
using System.Text; 
using WatsonWebserver; 

namespace Kvpbase.Classes
{
    /// <summary>
    /// Metadata for an incoming HTTP API request.
    /// </summary>
    public class RequestMetadata
    {
        #region Public-Members
         
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
         
        #endregion

        #region Public-Methods
          
        #endregion

        #region Private-Methods

        #endregion

        #region Public-Embedded-Classes

        /// <summary>
        /// Request parameters found in querystring key-value pairs and the URL itself.
        /// </summary>
        public class Parameters
        {
            #region Public-Members
             
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

            #endregion

            #region Private-Members

            #endregion

            #region Constructors-and-Factories

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
                if (req.RawUrlEntries.Count >= 1) ret.UserGuid = req.RawUrlEntries[0];

                if (req.RawUrlEntries.Count > 1) ret.ContainerName = req.RawUrlEntries[1];
                if (req.RawUrlEntries.Count > 2)
                {
                    string rawUrl = String.Copy(req.RawUrlWithoutQuery);
                    while (rawUrl.StartsWith("/")) rawUrl = rawUrl.Substring(1);
                    string[] vals = rawUrl.Split(new[] { '/' }, 3);
                    ret.ObjectKey = vals[2];
                }

                if (req.QuerystringEntries.ContainsKey("_auditlog")) ret.AuditLog = true; 
                if (req.QuerystringEntries.ContainsKey("_metadata")) ret.Metadata = true;
                if (req.QuerystringEntries.ContainsKey("_keys")) ret.Keys = true;
                if (req.QuerystringEntries.ContainsKey("_search")) ret.Search = true;
                if (req.QuerystringEntries.ContainsKey("_reqmetadata")) ret.RequestMetadata = true;
                if (req.QuerystringEntries.ContainsKey("_config")) ret.Config = true; 
                if (req.QuerystringEntries.ContainsKey("_html")) ret.Html = true;

                if (req.QuerystringEntries.ContainsKey("_auditkey"))
                    ret.AuditKey = req.QuerystringEntries["_auditkey"];

                if (req.QuerystringEntries.ContainsKey("_action"))
                    ret.Action = req.QuerystringEntries["_action"];

                if (req.QuerystringEntries.ContainsKey("_index"))
                {
                    if (Int32.TryParse(req.QuerystringEntries["_index"], out testInt))
                    {
                        if (testInt >= 0) ret.Index = testInt;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_count"))
                {
                    if (Int32.TryParse(req.QuerystringEntries["_count"], out testInt))
                    {
                        if (testInt >= 0) ret.Count = testInt;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_rename"))
                    ret.Rename = req.QuerystringEntries["_rename"];

                if (req.QuerystringEntries.ContainsKey("_createdbefore"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["_createdbefore"], out testTimestamp))
                    {
                        ret.CreatedBefore = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_createdafter"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["_createdafter"], out testTimestamp))
                    {
                        ret.CreatedAfter = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_updatedbefore"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["_updatedbefore"], out testTimestamp))
                    {
                        ret.UpdatedBefore = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_updatedafter"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["_updatedafter"], out testTimestamp))
                    {
                        ret.UpdatedAfter = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_accessedbefore"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["_accessedbefore"], out testTimestamp))
                    {
                        ret.LastAccessBefore = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_accessedafter"))
                {
                    if (DateTime.TryParse(req.QuerystringEntries["_accessedafter"], out testTimestamp))
                    {
                        ret.LastAccessAfter = testTimestamp;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_prefix"))
                    ret.Prefix = req.QuerystringEntries["_prefix"];

                if (req.QuerystringEntries.ContainsKey("_md5"))
                    ret.Md5 = req.QuerystringEntries["_md5"];

                if (req.QuerystringEntries.ContainsKey("_orderby"))
                    ret.OrderBy = req.QuerystringEntries["_orderby"];

                if (req.QuerystringEntries.ContainsKey("_contenttype"))
                    ret.ContentType = req.QuerystringEntries["_contenttype"];

                if (req.QuerystringEntries.ContainsKey("_tags"))
                    ret.Tags = req.QuerystringEntries["_tags"];

                if (req.QuerystringEntries.ContainsKey("_sizemin"))
                {
                    if (Int64.TryParse(req.QuerystringEntries["_sizemin"], out testLong))
                    {
                        ret.SizeMin = testLong;
                    }
                }

                if (req.QuerystringEntries.ContainsKey("_sizemax"))
                {
                    if (Int64.TryParse(req.QuerystringEntries["_sizemax"], out testLong))
                    {
                        ret.SizeMax = testLong;
                    }
                }
                 
                return ret;
            }

            #endregion
        }

        #endregion
    }
}
