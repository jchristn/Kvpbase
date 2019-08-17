using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse HttpGetContainer(RequestMetadata md)
        {  
            #region Retrieve-Container
            
            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                List<Node> nodes = new List<Node>();
                if (!_OutboundMessageHandler.FindContainerOwners(md, out nodes))
                {
                    _Logging.Warn("HttpGetContainer unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, 404, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(5, 404, "Unknown user or container.", null), true)));
                }
                else
                {
                    string redirectUrl = null;
                    HttpResponse redirectRest = _OutboundMessageHandler.BuildRedirectResponse(md, nodes[0], out redirectUrl);
                    _Logging.Debug("HttpGetContainer redirecting container " + md.Params.UserGuid + "/" + md.Params.Container + " to " + redirectUrl);
                    return redirectRest;
                }
            }
            
            bool isPublicRead = currContainer.IsPublicRead();

            #endregion

            #region Authenticate-and-Authorize

            if (!isPublicRead)
            {
                if (md.User == null || !(md.User.Guid.ToLower().Equals(md.Params.UserGuid.ToLower())))
                {
                    _Logging.Warn("HttpGetContainer unauthorized unauthenticated access attempt to container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, 401, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                }
            }
             
            if (md.Perm != null)
            {
                if (!md.Perm.ReadContainer)
                {
                    _Logging.Warn("HttpGetContainer unauthorized access attempt to container " + md.Params.UserGuid + "/" + md.Params.Container);
                    return new HttpResponse(md.Http, 401, null, "application/json",
                        Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(3, 401, "Unauthorized.", null), true)));
                }
            }

            #endregion

            #region Retrieve-Settings

            ContainerSettings settings = null;
            if (!_ContainerMgr.GetContainerSettings(md.Params.UserGuid, md.Params.Container, out settings))
            {
                _Logging.Warn("HttpGetContainer unable to retrieve settings for " + md.Params.UserGuid + "/" + md.Params.Container);
                return new HttpResponse(md.Http, 500, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(4, 500, null, null), true)));
            }

            if (md.Params.Config)
            {
                return new HttpResponse(md.Http, 200, null, "application/json", Encoding.UTF8.GetBytes(Common.SerializeJson(settings, true)));
            }

            #endregion

            #region Enumerate-and-Return

            int? index = null;
            if (md.Params.Index != null) index = Convert.ToInt32(md.Params.Index);

            int? count = null;
            if (md.Params.Count != null) count = Convert.ToInt32(md.Params.Count);
             
            ContainerMetadata meta = _ContainerHandler.Enumerate(md, currContainer, index, count, md.Params.OrderBy);
             
            if (md.Params.Html)
            {
                return new HttpResponse(md.Http, 200, null, "text/html", Encoding.UTF8.GetBytes(DirectoryListingPage(meta)));
            }
            else
            {
                return new HttpResponse(md.Http, 200, null, "application/json",
                    Encoding.UTF8.GetBytes(Common.SerializeJson(meta, true)));
            }

            #endregion 
        }

        public static string DirectoryListingPage(ContainerMetadata meta)
        {
            string ret =
                "<html>" +
                "   <head>" +
                "      <title>Kvpbase :: Directory of /" + meta.User + "/" + meta.Name + "</title>" +
                "      <style>" +
                "         body {" +
                "         font-family: arial;" +
                "         }" +
                "         pre {" +
                "         background-color: #e5e7ea;" +
                "         color: #333333; " +
                "         }" +
                "         h3 {" +
                "         color: #333333; " +
                "         padding: 4px;" +
                "         border: 4px;" +
                "         }" +
                "         p {" +
                "         color: #333333; " +
                "         padding: 4px;" +
                "         border: 4px;" +
                "         }" +
                "         a {" +
                "         color: #333333;" +
                "         padding: 4px;" +
                "         border: 4px;" +
                "         text-decoration: none; " +
                "         }" +
                "         li {" +
                "         padding: 6px;" +
                "         border: 6px;" +
                "         }" +
                "         td {" +
                "         padding: 4px;" +
                "         text-align: left;" +
                "         }" +
                "         tr {" +
                "         background-color: #ffffff;" +
                "         padding: 4px;" +
                "         text-align: left;" +
                "         }" +
                "         th {" +
                "         background-color: #444444;" +
                "         color: #ffffff;" +
                "         padding: 4px;" +
                "         text-align: left;" +
                "         }" +
                "      </style>" +
                "   </head>" +
                "   <body>" +
                "      <pre>" +
                WebUtility.HtmlEncode(Logo()) +
                "  	   </pre>" +
                "      <p>Directory of: /" + meta.User + "/" + meta.Name + "</p>" +
                "      <p>" +
                "      <table>" +
                "         <tr>" +
                "            <th>Object Key</th>" +
                "            <th>Content Type</th>" +
                "            <th>Size</th>" +
                "            <th>Created (UTC)</th>" +
                "         </tr>";

            if (meta.Objects != null && meta.Objects.Count > 0)
            { 
                foreach (ObjectMetadata obj in meta.Objects)
                {
                    // <a href='/foo/bar' target='_blank'>foo.bar</a>
                    ret +=
                        "         <tr>" +
                        "            <td><a href='/" + meta.User + "/" + meta.Name + "/" + obj.Key + "' target='_blank'>" + obj.Key + "</a></td>" +
                        "            <td>" + obj.ContentType + "</td>" +
                        "            <td>" + obj.ContentLength + "</td>" +
                        "            <td>" + Convert.ToDateTime(obj.CreatedUtc).ToString(_TimestampFormat) + "</td>" +
                        "         </tr>"; 
                }
            }

            ret +=
                "      </table>" +
                "      </p>" +
                "   </body>" +
                "</html>";

            return ret;
        }
    }
}
 