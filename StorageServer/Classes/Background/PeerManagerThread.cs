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
        public PeerManagerThread(Settings settings, Events logging, Topology topology, Node self)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (self == null) throw new ArgumentNullException(nameof(self));
            Task.Run(() => PeerManagerWorker(settings, logging, topology, self));
        }

        private void PeerManagerWorker(Settings settings, Events logging, Topology topology, Node self)
        {
            #region Setup

            if (topology == null || topology.IsEmpty())
            {
                logging.Log(LoggingModule.Severity.Debug, "PeerManagerWorker exiting, no topology");
                return;
            }

            if (settings.Topology.RefreshSec <= 0)
            {
                logging.Log(LoggingModule.Severity.Warn, "PeerManagerWorker setting topology refresh timer to 10 sec (config value too low: " + settings.Topology.RefreshSec + " sec)");
                settings.Topology.RefreshSec = 10;
            }

            logging.Log(LoggingModule.Severity.Debug, "PeerManagerWorker starting with topology refresh timer set to " + settings.Topology.RefreshSec + " sec");

            #endregion

            #region Process

            bool firstRun = true;
            while (true)
            {
                #region Wait

                if (!firstRun)
                {
                    Thread.Sleep(settings.Topology.RefreshSec * 1000);
                }
                else
                {
                    firstRun = false;
                }
                    
                #endregion

                #region Session-Variables

                List<Node> updatedNodeList = new List<Node>();
                List<Node> updatedNeighborList = new List<Node>();

                #endregion

                #region Process-the-List

                foreach (Node curr in topology.Nodes)
                {
                    #region Skip-if-Self

                    if (self.NodeId == curr.NodeId)
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
                        Common.IsTrue(settings.Rest.AcceptInvalidCerts),
                        Common.AddToDictionary(settings.Server.HeaderApiKey, settings.Server.AdminApiKey, null),
                        null);
                    
                    if (resp == null)
                    {
                        #region No-REST-Response

                        curr.NumFailures++;
                        curr.LastAttempt = DateTime.Now;
                        logging.Log(LoggingModule.Severity.Warn, "PeerManagerWorker null response connecting to " + url + " (" + curr.NumFailures + " failed attempts)");
                        updatedNodeList.Add(curr);
                        if (self.IsNeighbor(curr)) updatedNeighborList.Add(curr);
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
                            logging.Log(LoggingModule.Severity.Warn, "PeerManagerWorker non-200 (" + resp.StatusCode + ") response connecting to " + url + " (" + curr.NumFailures + " failed attempts)");
                            updatedNodeList.Add(curr);
                            if (self.IsNeighbor(curr)) updatedNeighborList.Add(curr);
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
                            if (self.IsNeighbor(curr)) updatedNeighborList.Add(curr);
                            continue;

                            #endregion
                        }
                    }

                    #endregion
                }

                #endregion

                #region Update-the-Lists

                topology.Nodes = updatedNodeList;
                topology.Replicas = updatedNeighborList;
                topology.LastProcessed = DateTime.Now;

                #endregion
            }

            #endregion
        }
    }
}