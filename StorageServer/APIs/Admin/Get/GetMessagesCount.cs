using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse GetMessagesCount(RequestMetadata md)
        {
            List<string> subdirectories = new List<string>();
            List<string> files = new List<string>();
            long bytes = 0;

            if (Common.WalkDirectory(
                _Settings.Environment,
                0,
                _Settings.Messages.Directory,
                false,
                out subdirectories,
                out files,
                out bytes,
                true))
            {
                return new HttpResponse(md.CurrHttpReq, true, 200, null, "text/plain", files.Count, true);
            }
            else
            {
                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json", null, true);
            }
        }
    }
}