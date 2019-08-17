using System;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Core;

namespace Kvpbase
{
    public partial class StorageServer
    {
        static HttpResponse AdminApiHandler(RequestMetadata md)
        {
            #region Enumerate

            _Logging.Debug(
                "AdminApiHandler admin API requested by " + 
                md.Http.SourceIp + ":" + md.Http.SourcePort + " " + 
                md.Http.Method + " " + md.Http.RawUrlWithoutQuery);

            #endregion

            #region Metadata

            if (md.Params.RequestMetadata)
            {
                RequestMetadata respMetadata = md.Sanitized();
                return new HttpResponse(md.Http, 200, null, "application/json", Encoding.UTF8.GetBytes(Common.SerializeJson(respMetadata, true)));
            }

            #endregion

            #region Process-Request

            switch (md.Http.Method)
            {
                case HttpMethod.GET:
                    #region get

                    if (md.Http.RawUrlWithoutQuery.Equals("/admin/disks"))
                    {
                        return HttpGetDisks(md);
                    }

                    if (md.Http.RawUrlWithoutQuery.Equals("/admin/topology"))
                    {
                        return HttpGetTopology(md);
                    }

                    break;

                #endregion

                case HttpMethod.PUT:
                    #region put

                    break;

                #endregion

                case HttpMethod.POST:
                    #region post
                     
                    break;

                #endregion

                case HttpMethod.DELETE:
                    #region delete
                     
                    break;

                #endregion

                case HttpMethod.HEAD:
                    #region head

                    break;

                #endregion 
            }

            _Logging.Warn("AdminApiHandler unknown endpoint URL: " + md.Http.RawUrlWithoutQuery);
            return new HttpResponse(md.Http, 400, null, "application/json",
                Encoding.UTF8.GetBytes(Common.SerializeJson(new ErrorResponse(2, 400, "Unknown endpoint.", null), true)));

            #endregion
        }
    }
}