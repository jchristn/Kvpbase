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

            req.UserGuid = md.CurrUser.Guid;
            req.Key = null;
            req.QueryTopology = true;
            req.Filters = null;
            req.ContainerPath = new List<string>();
            req.Urls = new List<string>();

            #endregion

            #region Retrieve-User-Home-Directory

            homeDirectory = _Users.GetHomeDirectory(req.UserGuid, _Settings);
            if (String.IsNullOrEmpty(homeDirectory))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "GetReplicas unable to retrieve home directory for user GUID " + req.UserGuid);
                return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to find home directory for user.", null).ToJson(), true);
            }

            #endregion

            #region Check-Locally

            if (Common.VerifyDirectoryAccess(_Settings.Environment, homeDirectory))
            {
                if (Common.IsTrue(_Node.Ssl))
                {
                    req.Urls.Add("https://" + _Node.DnsHostname + ":" + _Node.Port + "/" + req.UserGuid + "/");
                }
                else
                {
                    req.Urls.Add("http://" + _Node.DnsHostname + ":" + _Node.Port + "/" + req.UserGuid + "/");
                }
            }

            #endregion

            #region Check-Topology

            foreach (Node curr in _Topology.Nodes)
            {
                if (curr.NodeId == _Node.NodeId) continue;
                if (Node.FindObject(_Settings, curr, req))
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

            return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(req), true);
        }
    }
}