using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using SyslogLogging;
using RestWrapper;

namespace Kvpbase
{
    public class PeerManagerThread
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private Events _Logging;
        private Topology _Topology;
        private Node _Node;

        #endregion

        #region Constructors-and-Factories

        public PeerManagerThread(Settings settings, Events logging, Topology topology, Node self)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (self == null) throw new ArgumentNullException(nameof(self));

            _Settings = settings;
            _Logging = logging;
            _Topology = topology;
            _Node = self;

            if (_Topology == null || _Topology.IsEmpty())
            {
                _Logging.Log(LoggingModule.Severity.Debug, "PeerManagerThread exiting, no topology");
                return;
            }

            if (_Settings.Topology.RefreshSec <= 0)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "PeerManagerThread setting topology refresh timer to 10 sec, config value too low: " + settings.Topology.RefreshSec + " sec");
                _Settings.Topology.RefreshSec = 10;
            }

            Task.Run(() => PeerManagerWorker());
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        private void PeerManagerWorker()
        { 
            #region Process
             
            while (true)
            {
                #region Wait

                Task.Delay(_Settings.Topology.RefreshSec * 1000).Wait();
                    
                #endregion

                #region Session-Variables

                List<Node> updatedNodeList = new List<Node>();
                List<Node> updatedNeighborList = new List<Node>();

                #endregion

                #region Process-the-List

                foreach (Node curr in _Topology.Nodes)
                {
                    #region Skip-if-Self

                    if (_Node.NodeId == curr.NodeId)
                    {
                        updatedNodeList.Add(curr);
                        continue;
                    }

                    #endregion

                    #region Set-URL

                    string url = "";
                    if (Common.IsTrue(curr.Ssl))
                    {
                        url = "https://" + curr.DnsHostname + ":" + curr.Port + "/admin/heartbeat";
                    }
                    else
                    {
                        url = "http://" + curr.DnsHostname + ":" + curr.Port + "/admin/heartbeat";
                    }

                    #endregion

                    #region Process-REST-Request

                    RestWrapper.RestResponse resp = RestRequest.SendRequestSafe(
                        url, "application/json", "GET", null, null, false,
                        Common.IsTrue(_Settings.Rest.AcceptInvalidCerts),
                        Common.AddToDictionary(_Settings.Server.HeaderApiKey, _Settings.Server.AdminApiKey, null),
                        null);
                    
                    if (resp == null)
                    {
                        #region No-REST-Response

                        curr.NumFailures++;
                        curr.LastAttempt = DateTime.Now;
                        _Logging.Log(LoggingModule.Severity.Warn, "PeerManagerWorker null response connecting to " + url + " (" + curr.NumFailures + " failed attempts)");
                        updatedNodeList.Add(curr);
                        if (_Node.IsNeighbor(curr)) updatedNeighborList.Add(curr);
                        continue;

                        #endregion
                    }
                    else
                    {
                        if (resp.StatusCode != 200)
                        {
                            #region Failed-Heartbeat

                            curr.NumFailures++;
                            curr.LastAttempt = DateTime.Now;
                            _Logging.Log(LoggingModule.Severity.Warn, "PeerManagerWorker non-200 (" + resp.StatusCode + ") response connecting to " + url + " (" + curr.NumFailures + " failed attempts)");
                            updatedNodeList.Add(curr);
                            if (_Node.IsNeighbor(curr)) updatedNeighborList.Add(curr);
                            continue;

                            #endregion
                        }
                        else
                        {
                            #region Successful-Heartbeat

                            curr.NumFailures = 0;
                            curr.LastAttempt = DateTime.Now;
                            curr.LastSuccess = DateTime.Now;
                            updatedNodeList.Add(curr);
                            if (_Node.IsNeighbor(curr)) updatedNeighborList.Add(curr);
                            continue;

                            #endregion
                        }
                    }

                    #endregion
                }

                #endregion

                #region Update-the-Lists

                _Topology.Nodes = updatedNodeList;
                _Topology.Replicas = updatedNeighborList;
                _Topology.LastProcessed = DateTime.Now;

                #endregion
            }

            #endregion
        }

        #endregion
    }
}