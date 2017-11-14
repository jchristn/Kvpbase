﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse PostMove(RequestMetadata md)
        {
            #region Variables

            string containerVal = "";
            bool container = false;

            #endregion

            #region Get-Values-from-Querystring

            containerVal = md.CurrHttpReq.RetrieveHeaderValue("container");
            if (!String.IsNullOrEmpty(containerVal))
            {
                container = Common.IsTrue(containerVal);
            }

            #endregion

            #region Process

            if (container)
            {
                return _Container.Move(md);
            }
            else
            {
                return _Object.Move(md);
            }

            #endregion
        }
    }
}