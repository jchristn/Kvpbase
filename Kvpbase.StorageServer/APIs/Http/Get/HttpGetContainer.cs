using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using WatsonWebserver;
using Kvpbase.StorageServer.Classes;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer
{
    public partial class Program
    {
        internal static async Task HttpGetContainer(RequestMetadata md)
        {
            string header = _Header + md.Http.Request.SourceIp + ":" + md.Http.Request.SourcePort + " ";

            ContainerClient client = _ContainerMgr.GetContainerClient(md.Params.UserGuid, md.Params.ContainerName);
            if (client == null)
            { 
                _Logging.Warn(header + "HttpGetContainer unable to find container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                md.Http.Response.StatusCode = 404;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(5, 404, null, null), true));
                return;
            }
              
            if (!client.Container.IsPublicRead)
            {
                if (md.User == null || !(md.User.GUID.ToLower().Equals(md.Params.UserGuid.ToLower())))
                {
                    _Logging.Warn(header + "HttpGetContainer unauthorized unauthenticated access attempt to container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                    md.Http.Response.StatusCode = 401;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                    return;
                }
            }
             
            if (md.Perm != null)
            {
                if (!md.Perm.ReadContainer)
                {
                    _Logging.Warn(header + "HttpGetContainer unauthorized access attempt to container " + md.Params.UserGuid + "/" + md.Params.ContainerName);
                    md.Http.Response.StatusCode = 401;
                    md.Http.Response.ContentType = "application/json";
                    await md.Http.Response.Send(Common.SerializeJson(new ErrorResponse(3, 401, null, null), true));
                    return;
                }
            }
             
            if (md.Params.Config)
            {
                md.Http.Response.StatusCode = 200;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(client.Container, true));
                return;
            }
              
            EnumerationFilter filter = EnumerationFilter.FromRequestMetadata(md);

            ContainerMetadata meta = client.Enumerate(
                (int?)md.Params.Index,
                (int?)md.Params.Count,
                filter,
                md.Params.OrderBy);

            if (md.Params.Keys)
            {
                Dictionary<string, string> vals = new Dictionary<string, string>();
                vals = client.ReadContainerKeyValues();
                md.Http.Response.StatusCode = 200;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(vals, true));
                return;
            }
            else if (md.Params.Html)
            {
                md.Http.Response.StatusCode = 200;
                md.Http.Response.ContentType = "text/html";
                await md.Http.Response.Send(DirectoryListingPage(meta));
                return;
            }
            else
            {
                md.Http.Response.StatusCode = 200;
                md.Http.Response.ContentType = "application/json";
                await md.Http.Response.Send(Common.SerializeJson(meta, true));
                return;
            } 
        }

        internal static string DirectoryListingPage(ContainerMetadata meta)
        {
            string ret =
                "<html>" +
                "   <head>" +
                "      <title>Kvpbase :: Directory of /" + meta.UserGUID + "/" + meta.ContainerName + "</title>" +
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
                "      <p>Directory of: /" + meta.UserGUID + "/" + meta.ContainerName + "</p>" +
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
                        "            <td><a href='/" + meta.UserGUID + "/" + meta.ContainerName + "/" + obj.ObjectKey + "' target='_blank'>" + obj.ObjectKey + "</a></td>" +
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
 