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
        public static HttpResponse GetReplicas(RequestMetadata md)
        {
            #region Variables

            Find req = new Find();
            string homeDirectory = "";

            #endregion

            #region Populate-Find-Object

            req.UserGuid = md.CurrentUserMaster.Guid;
            req.Key = null;
            req.QueryTopology = true;
            req.Filters = null;
            req.ContainerPath = new List<string>();
            req.Urls = new List<string>();

            #endregion

            #region Retrieve-User-Home-Directory

            homeDirectory = Users.GetHomeDirectory(req.UserGuid, CurrentSettings, Logging);
            if (String.IsNullOrEmpty(homeDirectory))
            {
                Logging.Log(LoggingModule.Severity.Warn, "GetReplicas unable to retrieve home directory for user GUID " + req.UserGuid);
                return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to find home directory for user.", null).ToJson(), true);
            }

            #endregion

            #region Check-Locally

            if (Common.VerifyDirectoryAccess(CurrentSettings.Environment, homeDirectory))
            {
                if (Common.IsTrue(CurrentNode.Ssl))
                {
                    req.Urls.Add("https://" + CurrentNode.DnsHostname + ":" + CurrentNode.Port + "/" + req.UserGuid + "/");
                }
                else
                {
                    req.Urls.Add("http://" + CurrentNode.DnsHostname + ":" + CurrentNode.Port + "/" + req.UserGuid + "/");
                }
            }

            #endregion

            #region Check-Topology

            foreach (Node curr in CurrentTopology.Nodes)
            {
                if (curr.NodeId == CurrentNode.NodeId) continue;
                if (Node.FindObject(CurrentSettings, curr, req))
                {
                    if (Common.IsTrue(curr.Ssl))
                    {
                        req.Urls.Add("https://" + curr.DnsHostname + ":" + curr.Port + "/" + req.UserGuid + "/" + req.Key);
                    }
                    else
                    {
                        req.Urls.Add("http://" + curr.DnsHostname + ":" + curr.Port + "/" + req.UserGuid + "/" + req.Key);
                    }
                }
            }

            #endregion

            return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", Common.SerializeJson(req), true);
        }
    }
}