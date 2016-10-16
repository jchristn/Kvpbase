﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using RestWrapper;
using WatsonWebserver;

namespace Kvpbase
{
    public class ReplicationHandler
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings CurrentSettings;
        private Topology CurrentTopology;
        private Node CurrentNode;
        private UserManager Users;
        private Events Logging;

        #endregion

        #region Constructors-and-Factories

        public ReplicationHandler(Settings settings, Topology topology, Node node, UserManager users, Events logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (node == null) throw new ArgumentNullException(nameof(node));
            if (users == null) throw new ArgumentNullException(nameof(users));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            
            CurrentSettings = settings;
            CurrentTopology = topology;
            CurrentNode = node;
            Users = users;
            Logging = logging;
        }

        #endregion

        #region Public-Methods

        #region Client-Sender-Methods

        public bool ContainerDelete(Obj currObj, List<Node> nodes)
        {
            try
            {
                #region Check-for-Null-Values

                if (currObj == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete null value for currObj");
                    return false;
                }

                if (nodes == null) return true;
                if (nodes.Count < 1) return true;

                #endregion

                #region Variables

                Dictionary<string, string> headers = new Dictionary<string, string>();
                RestResponse resp = new RestResponse();
                string url = "";

                #endregion

                #region Build-Dictionaries

                headers.Add(CurrentSettings.Server.HeaderApiKey, CurrentSettings.Server.AdminApiKey);

                #endregion

                #region Process-Each-Node

                foreach (Node curr in nodes)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ContainerDelete removing container from node " + curr.Name + " (ID " + curr.NodeId + ")");

                    #region Reset-Variables

                    resp = new RestResponse();
                    url = "";

                    #endregion

                    #region Generate-URL

                    if (Common.IsTrue(curr.Ssl))
                    {
                        url = "https://" + curr.DnsHostname + ":" + curr.Port + "/admin/replication/container";
                    }
                    else
                    {
                        url = "http://" + curr.DnsHostname + ":" + curr.Port + "/admin/replication/container";
                    }

                    #endregion

                    #region Submit-Cleanup-Request

                    resp = RestRequest.SendRequestSafe(
                        url,
                        "application/json",
                        "DELETE",
                        null, null, false,
                        Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                        headers,
                        Encoding.UTF8.GetBytes(Common.SerializeJson(currObj)));

                    if (resp == null)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete null REST response while writing to " + url + ", queueing message to node ID " + curr.NodeId + " " + curr.Name);
                        Message.SendMessage(CurrentSettings, Logging, CurrentNode, curr, "DELETE /admin/replication/container", Common.SerializeJson(currObj));
                        continue;
                    }

                    if (resp.StatusCode != 200 && resp.StatusCode != 201)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete non-200/201 REST response from " + url + ", queueing message to node ID " + curr.NodeId + " " + curr.Name);
                        Message.SendMessage(CurrentSettings, Logging, CurrentNode, curr, "DELETE /admin/replication/container", Common.SerializeJson(currObj));
                        continue;
                    }

                    #endregion
                }

                return true;

                #endregion
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerDelete exception encountered");
                Logging.Exception("ContainerDelete", "Outer exception", e);
                return false;
            }
        }

        public bool ContainerMove(MoveRequest currMove)
        {
            try
            {
                #region Check-for-Null-Values

                if (currMove == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerMove null value for move object supplied");
                return false;
            }

            if (CurrentTopology.Replicas == null)
            {
                Logging.Log(LoggingModule.Severity.Debug, "ContainerMove null replica list in topology");
                return true;
            }

            if (CurrentTopology.Replicas.Count < 1)
            {
                Logging.Log(LoggingModule.Severity.Debug, "ContainerMove empty replica list in topology");
                return true;
            }

            #endregion

                #region Variables

                List<Node> successfulMoves = new List<Node>();

                #endregion

                #region Replication

                if (CurrentTopology.Replicas != null)
                {
                    foreach (Node curr in CurrentTopology.Replicas)
                    {
                        if (!ContainerMoveReplica(currMove, curr))
                        {
                            Logging.Log(LoggingModule.Severity.Warn, "ContainerMove failed replication to node ID " + curr.NodeId + " " + curr.Name + " for move operation");
                            Task.Run(() => ContainerMoveReplicaAsync(currMove, curr));
                        }
                        else
                        {
                            successfulMoves.Add(curr);
                        }
                    }
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerMove exception encountered");
                Logging.Exception("ContainerMove", "Outer exception", e);
                return false;
            }
        }

        public bool ContainerRename(RenameRequest currRename)
        {
            try
            {
                #region Check-for-Null-Values

                if (currRename == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerRename null value for rename object supplied");
                return false;
            }

            if (CurrentTopology.Replicas == null)
            {
                Logging.Log(LoggingModule.Severity.Debug, "ContainerRename null replica list in topology");
                return true;
            }

            if (CurrentTopology.Replicas.Count < 1)
            {
                Logging.Log(LoggingModule.Severity.Debug, "ContainerRename empty replica list in topology");
                return true;
            }

            #endregion

                #region Variables

                List<Node> successfulRenames = new List<Node>();

                #endregion

                #region Replication

                if (CurrentTopology.Replicas != null)
                {
                    foreach (Node curr in CurrentTopology.Replicas)
                    {
                        if (!ContainerRenameReplica(currRename, curr))
                        {
                            Logging.Log(LoggingModule.Severity.Warn, "ContainerRename failed replication to node ID " + curr.NodeId + " " + curr.Name + " for rename operation");
                            Task.Run(() => ContainerRenameReplicaAsync(currRename, curr));
                        }
                        else
                        {
                            successfulRenames.Add(curr);
                        }
                    }
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerRename exception encountered");
                Logging.Exception("ContainerRename", "Outer exception", e);
                return false;
            }
        }

        public bool ContainerWrite(Obj currObj, out List<Node> nodes)
        {
            nodes = new List<Node>();

            try
            {
                #region Check-for-Null-Values

                if (currObj == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite null path object supplied");
                    return false;
                }

                #endregion

                #region Variables

                List<Node> successfulWrites = new List<Node>();

                #endregion

                #region Check-Replication-Mode

                switch (currObj.ReplicationMode)
                {
                    case "none":
                        return true;

                    case "async":
                        if (CurrentTopology.Replicas != null)
                        {
                            nodes = Common.CopyObject<List<Node>>(CurrentTopology.Replicas);
                            Task.Run(() => ContainerWriteReplicaAsync(currObj));
                        }
                        return true;

                    case "sync":
                        if (CurrentTopology.Replicas != null)
                        {
                            nodes = Common.CopyObject<List<Node>>(CurrentTopology.Replicas);
                            currObj.Replicas = nodes;
                        }
                        break;

                    default:
                        Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite unknown replication mode in path object: " + currObj.ReplicationMode);
                        Common.ExitApplication("ContainerWrite", "Unknown replication mode", -1);
                        return false;
                }

                #endregion

                #region Sync-Replication

                if (CurrentTopology.Replicas != null)
                {
                    foreach (Node curr in CurrentTopology.Replicas)
                    {
                        if (!ContainerWriteReplica(currObj, curr))
                        {
                            Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite failed replication to node ID " + curr.NodeId);
                            Task.Run(() =>
                            {
                                ReplicationHandler rh = new ReplicationHandler(CurrentSettings, CurrentTopology, CurrentNode, Users, Logging);
                                rh.ContainerDelete(currObj, successfulWrites);
                            });
                            return false;
                        }
                        else
                        {
                            successfulWrites.Add(curr);
                        }
                    }
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerWrite exception encountered");
                Logging.Exception("ContainerWrite", "Outer exception", e);
                return false;
            }
        }

        public bool ObjectDelete(Obj currObj, List<Node> nodes)
        {
            try
            {
                #region Check-for-Null-Values

                if (currObj == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete null value for currObj");
                return false;
            }

            if (nodes == null) return true;
            if (nodes.Count < 1) return true;

            #endregion

                #region Variables

                Dictionary<string, string> headers = new Dictionary<string, string>();
                RestResponse resp = new RestResponse();
                string url = "";

                #endregion

                #region Build-Dictionaries

                headers.Add(CurrentSettings.Server.HeaderApiKey, CurrentSettings.Server.AdminApiKey);

                #endregion

                #region Process-Each-Node

                foreach (Node curr in nodes)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ObjectDelete removing object " + currObj.Key + " from node " + curr.Name + " (node ID " + curr.NodeId + ")");

                    #region Reset-Variables

                    resp = new RestResponse();
                    url = "";

                    #endregion

                    #region Generate-URL

                    if (Common.IsTrue(curr.Ssl))
                    {
                        url = "https://" + curr.DnsHostname + ":" + curr.Port + "/admin/replication/object";
                    }
                    else
                    {
                        url = "http://" + curr.DnsHostname + ":" + curr.Port + "/admin/replication/object";
                    }

                    #endregion

                    #region Null-Out-Value

                    currObj.Value = null;

                    #endregion

                    #region Submit-Cleanup-Request

                    resp = RestRequest.SendRequestSafe(
                        url,
                        "application/json",
                        "DELETE",
                        null, null, false,
                        Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                        headers,
                        Encoding.UTF8.GetBytes(Common.SerializeJson(currObj)));

                    if (resp == null)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete null REST response while writing to " + url + ", queueing message to node ID " + curr.NodeId + " " + curr.Name);
                        Message.SendMessage(CurrentSettings, Logging, CurrentNode, curr, "DELETE /admin/replication/object", Common.SerializeJson(currObj));
                        continue;
                    }

                    if (resp.StatusCode != 200 && resp.StatusCode != 201)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete non-200/201 REST response from " + url + ", queueing message to node ID " + curr.NodeId + " " + curr.Name);
                        Message.SendMessage(CurrentSettings, Logging, CurrentNode, curr, "DELETE /admin/replication/object", Common.SerializeJson(currObj));
                        continue;
                    }

                    #endregion
                }

                return true;

                #endregion
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectDelete exception encountered");
                Logging.Exception("ObjectDelete", "Outer exception", e);
                return false;
            }
        }

        public bool ObjectMove(MoveRequest currMove, Obj currObj)
        {
            try
            {
                #region Check-for-Null-Values

                if (currMove == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectMove null value for move object supplied");
                return false;
            }

            if (currObj == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectMove null obj supplied");
                return false;
            }

            if (currObj.Replicas == null)
            {
                Logging.Log(LoggingModule.Severity.Debug, "ObjectMove null replica list for supplied obj");
                return true;
            }

            if (currObj.Replicas.Count < 1)
            {
                Logging.Log(LoggingModule.Severity.Debug, "ObjectMove empty replica list for supplied obj");
                return true;
            }

            #endregion

                #region Variables

                List<Node> successfulMoves = new List<Node>();

                #endregion

                #region Check-Replication-Mode

                switch (currObj.ReplicationMode)
                {
                    case "none":
                        Logging.Log(LoggingModule.Severity.Debug, "ObjectMove none replication mode specified");
                        return true;

                    case "async":
                    case "sync":
                        Logging.Log(LoggingModule.Severity.Debug, "ObjectMove sync or async replication mode specified");
                        break;

                    default:
                        Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unknown replication mode in obj: " + currObj.ReplicationMode);
                        Common.ExitApplication("ObjectMove", "Unknown replication mode", -1);
                        return false;
                }

                #endregion

                #region Sync-Replication

                if (currObj.Replicas != null)
                {
                    foreach (Node curr in currObj.Replicas)
                    {
                        switch (currObj.ReplicationMode)
                        {
                            case "none":
                                Logging.Log(LoggingModule.Severity.Debug, "ObjectMove none replication mode specified");
                                continue;

                            case "async":
                                Logging.Log(LoggingModule.Severity.Debug, "ObjectMove async replication mode specified");
                                Task.Run(() => ObjectMoveReplicaAsync(currMove, curr));
                                break;

                            case "sync":
                                Logging.Log(LoggingModule.Severity.Debug, "ObjectMove sync replication mode specified");
                                if (!ObjectMoveReplica(currMove, curr))
                                {
                                    Logging.Log(LoggingModule.Severity.Warn, "ObjectMove failed replication to node ID " + curr.NodeId + " " + curr.Name + " for move operation");
                                    Task.Run(() => ObjectMoveReplicaAsync(currMove, curr));
                                }
                                else
                                {
                                    successfulMoves.Add(curr);
                                }
                                break;

                            default:
                                Logging.Log(LoggingModule.Severity.Warn, "ObjectMove unknown replication mode in obj: " + currObj.ReplicationMode);
                                Common.ExitApplication("ObjectMove", "Unknown replication mode", -1);
                                return false;
                        }
                    }
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectMove exception encountered");
                Logging.Exception("ObjectMove", "Outer exception", e);
                return false;
            }
        }

        public bool ObjectRename(RenameRequest currRename, Obj currObj)
        {
            try
            {
                #region Check-for-Null-Values

                if (currRename == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectRename null value for rename object supplied");
                return false;
            }

            if (currObj == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectRename null obj supplied");
                return false;
            }

            if (currObj.Replicas == null)
            {
                Logging.Log(LoggingModule.Severity.Debug, "ObjectRename null replica list for supplied obj");
                return true;
            }

            if (currObj.Replicas.Count < 1)
            {
                Logging.Log(LoggingModule.Severity.Debug, "ObjectRename empty replica list for supplied obj");
                return true;
            }

            #endregion

                #region Variables

                List<Node> successfulRenames = new List<Node>();

                #endregion

                #region Check-Replication-Mode

                switch (currObj.ReplicationMode)
                {
                    case "none":
                        Logging.Log(LoggingModule.Severity.Debug, "ObjectRename none replication mode specified");
                        return true;

                    case "async":
                    case "sync":
                        Logging.Log(LoggingModule.Severity.Debug, "ObjectRename sync or async replication mode specified");
                        break;

                    default:
                        Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unknown replication mode in obj: " + currObj.ReplicationMode);
                        Common.ExitApplication("ObjectRename", "Unknown replication mode", -1);
                        return false;
                }

                #endregion

                #region Sync-Replication

                if (currObj.Replicas != null)
                {
                    foreach (Node currNode in currObj.Replicas)
                    {
                        switch (currObj.ReplicationMode)
                        {
                            case "none":
                                Logging.Log(LoggingModule.Severity.Debug, "ObjectRename none replication mode specified");
                                continue;

                            case "async":
                                Logging.Log(LoggingModule.Severity.Debug, "ObjectRename async replication mode specified");
                                Task.Run(() => ObjectRenameReplicaAsync(currRename, currNode));
                                break;

                            case "sync":
                                Logging.Log(LoggingModule.Severity.Debug, "ObjectRename sync replication mode specified");
                                if (!ObjectRenameReplica(currRename, currNode))
                                {
                                    Logging.Log(LoggingModule.Severity.Warn, "ObjectRename failed replication to node ID " + currNode.NodeId + " " + currNode.Name + " for rename operation");
                                    Task.Run(() => ObjectRenameReplicaAsync(currRename, currNode));
                                }
                                else
                                {
                                    successfulRenames.Add(currNode);
                                }
                                break;

                            default:
                                Logging.Log(LoggingModule.Severity.Warn, "ObjectRename unknown replication mode in obj: " + currObj.ReplicationMode);
                                Common.ExitApplication("ObjectRename", "Unknown replication mode", -1);
                                return false;
                        }
                    }
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectRename exception encountered");
                Logging.Exception("ObjectRename", "Outer exception", e);
                return false;
            }
        }

        public bool ObjectWrite(Obj currObj, out List<Node> nodes)
        {
            nodes = new List<Node>();

            try
            {
                #region Check-for-Null-Values

                if (currObj == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite null value detected for path");
                    return false;
                }
                
                #endregion

                #region Variables

                List<Node> successfulWrites = new List<Node>();

                #endregion

                #region Copy-Path-into-Object

                if (currObj.ContainerPath == null)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite path container path is null");
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite path container path has " + currObj.ContainerPath.Count + " entries");
                }

                #endregion

                #region Check-Replication-Mode

                Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite replication mode set to " + currObj.ReplicationMode);
                switch (currObj.ReplicationMode)
                {
                    case "none":
                        Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite replication set to none");
                        return true;

                    case "async":
                        if (CurrentTopology.Replicas != null)
                        {
                            nodes = Common.CopyObject<List<Node>>(CurrentTopology.Replicas);
                            currObj.Replicas = nodes;
                            Task.Run(() => ObjectWriteReplicaAsync(currObj));
                        }
                        return true;

                    case "sync":
                        if (CurrentTopology.Replicas != null)
                        {
                            nodes = Common.CopyObject<List<Node>>(CurrentTopology.Replicas);
                            currObj.Replicas = nodes;
                        }
                        break;

                    default:
                        Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite unknown replication mode in obj: " + currObj.ReplicationMode);
                        Common.ExitApplication("ObjectWrite", "Unknown replication mode", -1);
                        return false;
                }

                #endregion

                #region Sync-Replication

                if (CurrentTopology.Replicas != null)
                {
                    foreach (Node currNode in CurrentTopology.Replicas)
                    {
                        if (!ObjectWriteReplica(currObj, currNode))
                        {
                            Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite failed replication to node ID " + currNode.NodeId + " " + currNode.Name + " for key " + currObj.Key);
                            Task.Run(() =>
                            {
                                ReplicationHandler rh = new ReplicationHandler(CurrentSettings, CurrentTopology, CurrentNode, Users, Logging);
                                rh.ObjectDelete(currObj, successfulWrites);
                            });
                            return false;
                        }
                        else
                        {
                            successfulWrites.Add(currNode);
                        }
                    }
                }

                #endregion

                Logging.Log(LoggingModule.Severity.Debug, "ObjectWrite replicated to " + successfulWrites.Count + " nodes");
                return true;
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectWrite exception encountered");
                Logging.Exception("ObjectWrite", "Outer exception", e);
                return false;
            }
        }

        #endregion

        #region Server-Receive-Methods

        public HttpResponse ServerObjectMove(RequestMetadata md)
        {
            try
            {
                #region Deserialize-and-Initialize

                MoveRequest req = new MoveRequest();
                try
                {
                    req = Common.DeserializeJson<MoveRequest>(md.CurrentHttpRequest.Data);
                    if (req == null)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMove null request after deserialization, returning 400");
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                    }
                }
                catch (Exception)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMove unable to deserialize request body");
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                }

                #endregion

                #region Process

                if (ServerObjectMoveInternal(req))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ServerObjectMove successfully processed move request");
                    return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", null, true);
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMove unable to process move request");
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to process move request.", null).ToJson(), true);
                }

                #endregion
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMove exception encountered");
                Logging.Exception("ServerObjectMove", "Outer exception", e);
                return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to process move request.", null).ToJson(), true);
            }
        }

        public HttpResponse ServerObjectRename(RequestMetadata md)
        {
            try
            {
                #region Deserialize-and-Initialize

                RenameRequest req = new RenameRequest();
                try
                {
                    req = Common.DeserializeJson<RenameRequest>(md.CurrentHttpRequest.Data);
                    if (req == null)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRename null request after deserialization, returning 400");
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                    }
                }
                catch (Exception)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRename unable to deserialize request body");
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                }

                #endregion

                #region Process

                if (ServerObjectRenameInternal(req))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ServerObjectRename successfully processed rename request");
                    return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", null, true);
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRename unable to process rename request");
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to process rename request.", null).ToJson(), true);
                }

                #endregion
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRename exception encountered");
                Logging.Exception("ServerObjectRename", "Outer exception", e);
                return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to process move request.", null).ToJson(), true);
            }
        }

        public HttpResponse ServerContainerMove(RequestMetadata md)
        {
            try
            {
                #region Deserialize-and-Initialize

                MoveRequest req = new MoveRequest();
                try
                {
                    req = Common.DeserializeJson<MoveRequest>(md.CurrentHttpRequest.Data);
                    if (req == null)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMove null request after deserialization, returning 400");
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                            true);
                    }
                }
                catch (Exception)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMove unable to deserialize request body");
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                        true);
                }

                #endregion

                #region Process

                if (ServerContainerMoveInternal(req))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ServerContainerMove successfully processed move request");
                    return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", null, true);
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMove unable to process move request");
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to process move request.", null).ToJson(),
                        true);
                }

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMove exception encountered");
                Logging.Exception("ServerContainerMove", "Outer exception", e);
                return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to process move request.", null).ToJson(), true);
            }
        }

        public HttpResponse ServerContainerRename(RequestMetadata md)
        {
            try
            {
                #region Deserialize-and-Initialize

                RenameRequest req = new RenameRequest();
                try
                {
                    req = Common.DeserializeJson<RenameRequest>(md.CurrentHttpRequest.Data);
                    if (req == null)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRename null request after deserialization, returning 400");
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                    }
                }
                catch (Exception)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRename unable to deserialize request body");
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);

                }

                #endregion

                #region Process

                if (ServerContainerRenameInternal(req))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ServerContainerRename successfully processed rename request");
                    return new HttpResponse(md.CurrentHttpRequest, true, 200, null, "application/json", null, true);
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRename unable to process rename request");
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to process rename request.", null).ToJson(), true);
                }

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRename exception encountered");
                Logging.Exception("ServerContainerRename", "Outer exception", e);
                return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to process move request.", null).ToJson(), true);
            }
        }

        public HttpResponse ServerObjectReceive(RequestMetadata md)
        {
            try
            {
                #region Deserialize-and-Initialize

                Obj req = new Obj();
                try
                {
                    req = Common.DeserializeJson<Obj>(md.CurrentHttpRequest.Data);
                    if (req == null)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceive null request after deserialization, returning 400");
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                    }
                }
                catch (Exception)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceive unable to deserialize request body");
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(), true);
                }

                #endregion

                #region Retrieve-User

                UserMaster currUser = Users.GetUserByGuid(req.UserGuid);
                if (currUser == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceive unable to retrieve user for GUID " + req.UserGuid);
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to find user in request.", null).ToJson(), true);
                }

                #endregion

                #region Overwrite-Path-in-Path-Object

                req.DiskPath = Obj.BuildDiskPath(req, currUser, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(req.DiskPath))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceive unable to build disk path from request");
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(), true);
                }

                #endregion

                #region Process

                if (ServerObjectReceiveInternal(req))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ServerObjectReceive successfully stored " + req.Key);
                    return new HttpResponse(md.CurrentHttpRequest, true, 201, null, "application/json", null, true);
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceive unable to store " + req.Key);
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to store object.", null).ToJson(), true);
                }

                #endregion
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceive exception encountered");
                Logging.Exception("ServerObjectReceive", "Outer exception", e);
                return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to process move request.", null).ToJson(), true);
            }
        }

        public HttpResponse ServerContainerReceive(RequestMetadata md)
        {
            try
            {
                #region Deserialize-and-Initialize

                Obj req = new Obj();
                try
                {
                    req = Common.DeserializeJson<Obj>(md.CurrentHttpRequest.Data);
                    if (req == null)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ServerContainerReceive null request after deserialization, returning 400");
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                            true);
                    }
                }
                catch (Exception)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerReceive unable to deserialize request body");
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                        true);
                }

                #endregion

                #region Retrieve-User

                UserMaster currUser = Users.GetUserByGuid(req.UserGuid);
                if (currUser == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerReceive unable to retrieve user for GUID " + req.UserGuid);
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to find user in request.", null).ToJson(), true);
                }

                #endregion

                #region Overwrite-Path-in-Path-Object

                req.DiskPath = Obj.BuildDiskPath(req, currUser, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(req.DiskPath))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerReceive unable to build disk path from request");
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(),
                        true);
                }

                #endregion

                #region Process

                if (ServerContainerReceiveInternal(req))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ServerContainerReceive successfully wrote " + req.DiskPath);
                    return new HttpResponse(md.CurrentHttpRequest, true, 201, null, "application/json", null, true);
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerReceive unable to store " + req.Key);
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(4, 500, "Unable to store container.", null).ToJson(),
                        true);
                }

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerContainerReceive exception encountered");
                Logging.Exception("ServerContainerReceive", "Outer exception", e);
                return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                    new ErrorResponse(4, 500, "Unable to process move request.", null).ToJson(), true);
            }
        }

        //
        // These internal methods must be marked as public as they are used elsewhere
        //

        public bool ServerObjectMoveInternal(MoveRequest currMove)
        {
            try
            {
                #region Check-for-Null-Values

                if (currMove == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal null value for currMove");
                return false;
            }

            #endregion

                #region Variables

                Obj currObj = new Obj();

                #endregion

                #region Check-for-Unsafe-Characters

                if (MoveRequest.UnsafeFsChars(currMove))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal unsafe characters detected in request");
                    return false;
                }

                #endregion

                #region Check-if-Original-Object-Exists

                string diskPathOriginalObj = MoveRequest.BuildDiskPath(currMove, true, true, Users, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(diskPathOriginalObj))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal unable to build disk path for original object");
                    return false;
                }

                if (!Common.FileExists(diskPathOriginalObj))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal from object does not exist: " + diskPathOriginalObj);
                    return false;
                }

                #endregion

                #region Check-if-Target-Container-Exists

                string diskPathTargetContainer = MoveRequest.BuildDiskPath(currMove, false, false, Users, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(diskPathTargetContainer))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal unable to build disk path for target container");
                    return false;
                }

                if (!Common.DirectoryExists(diskPathTargetContainer))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal target container does not exist: " + diskPathOriginalObj);
                    return false;
                }

                #endregion

                #region Check-if-Target-Object-Exists

                string diskPathTargetObj = MoveRequest.BuildDiskPath(currMove, false, true, Users, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(diskPathTargetObj))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal unable to build disk path for target object");
                    return false;
                }

                if (Common.FileExists(diskPathTargetObj))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal target object already exists: " + diskPathOriginalObj);
                    return false;
                }

                #endregion

                #region Read-Object

                currObj = Obj.BuildObjFromDisk(diskPathOriginalObj, Users, CurrentSettings, CurrentTopology, CurrentNode, Logging);
                if (currObj == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal unable to retrieve obj for " + diskPathOriginalObj);
                    return false;
                }

                #endregion

                #region Perform-Move

                if (!Common.MoveFile(diskPathOriginalObj, diskPathTargetObj))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal unable to move file from " + diskPathOriginalObj + " to " + diskPathTargetObj);
                    return false;
                }

                #endregion

                #region Perform-Background-Rewrite

                if (!Common.IsTrue(currObj.GatewayMode))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "PostReplicationMoveObject spawning background task to rewrite object with correct metadata");
                    Task.Run(() => RewriteObject(diskPathTargetObj));
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerObjectMoveInternal exception encountered");
                Logging.Exception("ServerObjectMoveInternal", "Outer exception", e);
                return false;
            }
        }

        public bool ServerObjectRenameInternal(RenameRequest currRename)
        {
            try
            {
                #region Check-for-Null-Values

                if (currRename == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRenameInternal null value for currRename");
                return false;
            }

            #endregion

                #region Variables

                Obj currObj = new Obj();

                #endregion

                #region Check-for-Unsafe-Characters

                if (RenameRequest.UnsafeFsChars(currRename))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRenameInternal unsafe characters detected in request");
                    return false;
                }

                #endregion

                #region Check-if-Original-Exists

                string diskPathOriginal = RenameRequest.BuildDiskPath(currRename, true, Users, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(diskPathOriginal))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRenameInternal unable to build disk path for original object");
                    return false;
                }

                if (!Common.FileExists(diskPathOriginal))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRenameInternal from object does not exist: " + diskPathOriginal);
                    return false;
                }

                #endregion

                #region Check-if-Target-Exists

                string diskPathTarget = RenameRequest.BuildDiskPath(currRename, false, Users, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(diskPathTarget))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRenameInternal unable to build disk path for target object");
                    return false;
                }

                if (Common.FileExists(diskPathTarget))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRenameInternal target object already exists: " + diskPathOriginal);
                    return false;
                }

                #endregion

                #region Read-Object

                currObj = Obj.BuildObjFromDisk(diskPathOriginal, Users, CurrentSettings, CurrentTopology, CurrentNode, Logging);
                if (currObj == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "PostReplicationRenameObject unable to retrieve obj for " + diskPathOriginal);
                    return false;
                }

                #endregion

                #region Perform-Rename

                if (!Common.RenameFile(diskPathOriginal, diskPathTarget))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRenameInternal unable to rename file from " + diskPathOriginal + " to " + diskPathTarget);
                    return false;
                }

                #endregion

                #region Perform-Background-Rewrite

                if (!Common.IsTrue(currObj.GatewayMode))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "PostReplicationRenameObject spawning background task to rewrite object with correct metadata");
                    Task.Run(() => RewriteObject(diskPathTarget));
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerObjectRenameInternal exception encountered");
                Logging.Exception("ServerObjectRenameInternal", "Outer exception", e);
                return false;
            }
        }

        public bool ServerContainerMoveInternal(MoveRequest currMove)
        {
            try
            {
                #region Check-for-Null-Values

                if (currMove == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal null value for currMove");
                return false;
            }

            #endregion

                #region Variables

                bool userGatewayMode = false;

                #endregion

                #region Validate-Request-Body

                if (currMove.FromContainer == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal null value supplied for FromContainer, returning 400");
                    return false;
                }

                if (currMove.ToContainer == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal null value supplied for ToContainer, returning 400");
                    return false;
                }

                if (String.IsNullOrEmpty(currMove.MoveFrom))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal null value supplied for MoveFrom, returning 400");
                    return false;
                }

                if (String.IsNullOrEmpty(currMove.MoveTo))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal null value supplied for MoveTo, returning 400");
                    return false;
                }

                if (MoveRequest.UnsafeFsChars(currMove))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal unsafe characters detected in request, returning 400");
                    return false;
                }

                #endregion

                #region Check-if-Original-Exists

                string diskPathOriginal = MoveRequest.BuildDiskPath(currMove, true, true, Users, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(diskPathOriginal))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal unable to build disk path for original container");
                    return false;
                }

                if (!Common.DirectoryExists(diskPathOriginal))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal from container does not exist: " + diskPathOriginal);
                    return false;
                }

                #endregion

                #region Check-if-Target-Parent-Exists

                string diskPathTargetParent = MoveRequest.BuildDiskPath(currMove, false, false, Users, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(diskPathTargetParent))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal unable to build disk path for target container");
                    return false;
                }

                if (!Common.DirectoryExists(diskPathTargetParent))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal target parent container does not exist: " + diskPathOriginal);
                    return false;
                }

                #endregion

                #region Check-if-Target-Child-Exists

                string diskPathTargetChild = MoveRequest.BuildDiskPath(currMove, false, true, Users, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(diskPathTargetChild))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal unable to build disk path for target container");
                    return false;
                }

                if (Common.FileExists(diskPathTargetChild))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal target container already exists: " + diskPathOriginal);
                    return false;
                }

                #endregion

                #region Set-Gateway-Mode

                userGatewayMode = Users.GetGatewayMode(currMove.UserGuid, CurrentSettings);

                #endregion

                #region Move-Directory

                if (!Common.MoveDirectory(diskPathOriginal, diskPathTargetChild))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal unable to move container from " + diskPathOriginal + " to " + diskPathTargetChild);
                    return false;
                }

                #endregion

                #region Perform-Background-Rewrite

                if (!userGatewayMode)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "PostReplicationMoveContainer spawning background task to rewrite objects with correct metadata");
                    Task.Run(() => RewriteTree(diskPathTargetChild));
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerContainerMoveInternal exception encountered");
                Logging.Exception("ServerContainerMoveInternal", "Outer exception", e);
                return false;
            }
        }

        public bool ServerContainerRenameInternal(RenameRequest currRename)
        {
            try
            {
                #region Check-for-Null-Values

                if (currRename == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal null value for currRename");
                return false;
            }

            #endregion

                #region Variables

                bool userGatewayMode = false;

                #endregion

                #region Validate-Request-Body

                if (currRename.ContainerPath == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal null value supplied for ContainerPath, returning 400");
                    return false;
                }

                if (String.IsNullOrEmpty(currRename.RenameFrom))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal null value supplied for RenameFrom, returning 400");
                    return false;
                }

                if (String.IsNullOrEmpty(currRename.RenameTo))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal null value supplied for RenameTo, returning 400");
                    return false;
                }

                if (RenameRequest.UnsafeFsChars(currRename))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal unsafe characters detected in request, returning 400");
                    return false;
                }

                #endregion

                #region Check-if-Original-Exists

                string diskPathOriginal = RenameRequest.BuildDiskPath(currRename, true, Users, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(diskPathOriginal))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal unable to build disk path for original container");
                    return false;
                }

                if (!Common.DirectoryExists(diskPathOriginal))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal from container does not exist: " + diskPathOriginal);
                    return false;
                }

                #endregion

                #region Check-if-Target-Exists

                string diskPathTarget = RenameRequest.BuildDiskPath(currRename, false, Users, CurrentSettings, Logging);
                if (String.IsNullOrEmpty(diskPathTarget))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal unable to build disk path for target container");
                    return false;
                }

                if (Common.FileExists(diskPathTarget))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal target container already exists: " + diskPathOriginal);
                    return false;
                }

                #endregion

                #region Set-Gateway-Mode

                userGatewayMode = Users.GetGatewayMode(currRename.UserGuid, CurrentSettings);

                #endregion

                #region Rename-Directory

                if (!Common.RenameDirectory(diskPathOriginal, diskPathTarget))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal unable to rename container from " + diskPathOriginal + " to " + diskPathTarget);
                    return false;
                }

                #endregion

                #region Perform-Background-Rewrite

                if (!Common.IsTrue(userGatewayMode))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "PostReplicationRenameContainer spawning background task to rewrite objects with correct metadata");
                    Task.Run(() => RewriteTree(diskPathTarget));
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerContainerRenameInternal exception encountered");
                Logging.Exception("ServerContainerRenameInternal", "Outer exception", e);
                return false;
            }
        }

        public bool ServerObjectReceiveInternal(Obj currObj)
        {
            try
            {
                #region Check-for-Null-Values

                if (currObj == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceiveInternal null obj supplied");
                return false;
            }

            #endregion

                #region Retrieve-Home-Directory

                string homeDirectory = Users.GetHomeDirectory(currObj.UserGuid, CurrentSettings);
                if (String.IsNullOrEmpty(homeDirectory))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceiveInternal unable to retrieve home directory for user GUID " + currObj.UserGuid);
                    return false;
                }

                #endregion

                #region Create-Directories-if-Needed

                // create home directory if needed
                if (!Common.DirectoryExists(homeDirectory))
                {
                    Common.CreateDirectory(homeDirectory);
                }

                // now add each element in the path
                if (currObj.ContainerPath != null)
                {
                    if (currObj.ContainerPath.Count > 0)
                    {
                        foreach (string curr_container in currObj.ContainerPath)
                        {
                            homeDirectory += Common.GetPathSeparator(CurrentSettings.Environment) + curr_container;
                            if (!Common.DirectoryExists(homeDirectory))
                            {
                                Common.CreateDirectory(homeDirectory);
                            }
                        }
                    }
                }

                #endregion

                #region Delete-if-Exists

                if (Common.FileExists(currObj.DiskPath))
                {
                    if (!Common.DeleteFile(currObj.DiskPath))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceiveInternal file " + currObj.DiskPath + " already exists and was unable to be deleted");
                        return false;
                    }
                }

                #endregion

                #region Write-Expiration-Object

                if (currObj.Expiration != null)
                {
                    Obj expObj = Common.CopyObject<Obj>(currObj);
                    expObj.Value = null;

                    string expiration_filename =
                        Convert.ToDateTime(expObj.Expiration).ToString("MMddyyyy-hhmmss") +
                        "-" + Common.RandomString(8) + "-" + expObj.Key;

                    if (!Common.WriteFile(CurrentSettings.Expiration.Directory + expiration_filename, Common.SerializeJson(expObj), false))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceiveInternal unable to create expiration object " + expiration_filename);
                        return false;
                    }
                }

                #endregion

                #region Write-File

                if (!Common.IsTrue(currObj.GatewayMode))
                {
                    if (!Common.WriteFile(currObj.DiskPath, Common.SerializeJson(currObj), false))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceiveInternal unable to write replica to " + currObj.DiskPath);
                        return false;
                    }
                }
                else
                {
                    if (!Common.WriteFile(currObj.DiskPath, currObj.Value))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceiveInternal unable to write replica to " + currObj.DiskPath);
                        return false;
                    }
                }

                Logging.Log(LoggingModule.Severity.Debug, "ServerObjectReceiveInternal successfully wrote replica to " + currObj.DiskPath);
                return true;

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerObjectReceiveInternal exception encountered");
                Logging.Exception("ServerObjectReceiveInternal", "Outer exception", e);
                return false;
            }
        }

        public bool ServerContainerReceiveInternal(Obj currObj)
        {
            try
            {
                #region Check-for-Null-Values

                if (currObj == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerContainerReceiveInternal null path object supplied");
                return false;
            }

            #endregion

                #region Retrieve-User-Home-Directory

                string homeDirectory = Users.GetHomeDirectory(currObj.UserGuid, CurrentSettings);
                if (String.IsNullOrEmpty(homeDirectory))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ServerContainerReceiveInternal unable to retrieve home directory for user GUID " + currObj.UserGuid);
                    return false;
                }

                #endregion

                #region Create-Folder-if-Needed

                if (!Common.DirectoryExists(homeDirectory)) Common.CreateDirectory(homeDirectory);

                // now add each element in the path
                string currDirectory = String.Copy(homeDirectory);

                if (currObj.ContainerPath != null)
                {
                    if (currObj.ContainerPath.Count > 0)
                    {
                        foreach (string curr_container in currObj.ContainerPath)
                        {
                            currDirectory += Common.GetPathSeparator(CurrentSettings.Environment) + curr_container;
                            if (!Common.DirectoryExists(currDirectory))
                            {
                                Common.CreateDirectory(currDirectory);
                            }
                        }
                    }
                }

                return true;

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ServerContainerReceiveInternal exception encountered");
                Logging.Exception("ServerContainerReceiveInternal", "Outer exception", e);
                return false;
            }
        }

        #endregion

        #endregion

        #region Private-Methods

        #region Client-Sender-Methods

        private void SendReplica(Node to, string subject, string data)
        {
            try
            {
                #region Variables

                Message currMessage = new Message();
                bool success = false;
                string req = "";

                #endregion

                #region Setup

                currMessage.From = CurrentNode;
                currMessage.To = to;
                currMessage.Subject = subject;
                currMessage.Data = data;
                currMessage.Created = DateTime.Now;

                #endregion

                #region Set-URL

                string url = "";
                if (Common.IsTrue(currMessage.To.Ssl))
                {
                    url = "https://" + currMessage.To.DnsHostname + ":" + currMessage.To.Port + "/admin/message";
                }
                else
                {
                    url = "http://" + currMessage.To.DnsHostname + ":" + currMessage.To.Port + "/admin/message";
                }

                #endregion

                #region Attempt-to-Send

                req = Common.SerializeJson(currMessage);

                RestWrapper.RestResponse resp = RestRequest.SendRequestSafe(
                    url, "application/json", "POST", null, null, false,
                    Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                    Common.AddToDictionary(CurrentSettings.Server.HeaderApiKey, CurrentSettings.Server.AdminApiKey, null),
                    Encoding.UTF8.GetBytes(req));

                if (resp == null)
                {
                    #region No-REST-Response

                    Logging.Log(LoggingModule.Severity.Warn, "SendReplica null response connecting to " + url + ", message will be queued");
                    success = false;

                    #endregion
                }
                else
                {
                    if (resp.StatusCode != 200)
                    {
                        #region Failed-Message

                        Logging.Log(LoggingModule.Severity.Warn, "SendReplica non-200 response connecting to " + url + ", message will be queued");
                        success = false;

                        #endregion
                    }
                    else
                    {
                        #region Successful-Message

                        success = true;

                        #endregion
                    }
                }

                #endregion

                #region Store-if-Needed

                if (!success)
                {
                    #region Create-Directory-if-Needed

                    if (!Common.DirectoryExists(CurrentSettings.Replication.Directory + to.Name))
                    {
                        try
                        {
                            Common.CreateDirectory(CurrentSettings.Replication.Directory + to.Name);
                        }
                        catch (Exception e)
                        {
                            Logging.Log(LoggingModule.Severity.Warn, "SendReplica exception while creating directory " + CurrentSettings.Replication.Directory + to.Name);
                            Logging.Exception("SendReplica", "exception while creating directory " + CurrentSettings.Replication.Directory + to.Name, e);
                            Common.ExitApplication("SendReplica", "Unable to create directory", -1);
                            return;
                        }
                    }

                    #endregion

                    #region Generate-New-GUID

                    int loopCount = 0;
                    string guid = "";

                    while (true)
                    {
                        guid = Guid.NewGuid().ToString();
                        if (!Common.FileExists(CurrentSettings.Replication.Directory + to.Name + Common.GetPathSeparator(CurrentSettings.Environment) + guid))
                        {
                            break;
                        }

                        loopCount++;

                        if (loopCount > 16)
                        {
                            Logging.Log(LoggingModule.Severity.Warn, "SendReplica unable to generate unused GUID for folder " + CurrentSettings.Replication.Directory + to.Name + ", exiting");
                            Common.ExitApplication("SendReplica", "Unable to generate unused GUID", -1);
                            return;
                        }
                    }

                    #endregion

                    #region Write-File

                    if (!Common.WriteFile(
                        CurrentSettings.Replication.Directory + to.Name + Common.GetPathSeparator(CurrentSettings.Environment) + guid,
                        Common.SerializeJson(currMessage),
                        false))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "SendReplica unable to write message to " + CurrentSettings.Replication.Directory + to.Name + Common.GetPathSeparator(CurrentSettings.Environment) + guid + ", exiting");
                        Common.ExitApplication("SendReplica", "Unable to write message", -1);
                        return;
                    }

                    Logging.Log(LoggingModule.Severity.Debug, "SendReplica queued message to " + CurrentSettings.Replication.Directory + to.Name + Common.GetPathSeparator(CurrentSettings.Environment) + guid);

                    #endregion
                }

                #endregion

                return;
            }
            catch (Exception e)
            {
                Logging.Exception("SendReplica", "Outer exception", e);
                return;
            }
        }

        private void ContainerMoveReplicaAsync(MoveRequest currMove, Node currNode)
        {
            try
            {
                #region Check-for-Null-Values

                if (currMove == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerMoveReplicaAsync null value for currMove");
                return;
            }

            if (currNode == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerMoveReplicaAsync null value for currNode");
                return;
            }

            #endregion

                #region Process

                if (!ContainerMoveReplica(currMove, currNode))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ContainerMoveReplicaAsync unable to replicate move operation to node ID " + currNode.NodeId + " " + currNode.Name);
                    SendReplica(currNode, "POST /admin/replication/move/container", Common.SerializeJson(currMove));
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ContainerMoveReplicaAsync successfully replicated move operation to node ID " + currNode.NodeId + " " + currNode.Name);
                }

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ContainerMoveReplicaAsync", "Outer exception", e);
                return;
            }
        }

        private bool ContainerMoveReplica(MoveRequest currMove, Node currNode)
        {
            try
            {
                #region Check-for-Null-Values

                if (currMove == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerMoveReplica null value for currMove");
                return false;
            }

            if (currNode == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerMoveReplica null value for currNode");
                return false;
            }

            #endregion

                #region Variables

                Dictionary<string, string> headers = new Dictionary<string, string>();
                RestResponse resp = new RestResponse();
                string url = "";

                #endregion

                #region Generate-URL

                if (Common.IsTrue(currNode.Ssl))
                {
                    url = "https://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/move/container";
                }
                else
                {
                    url = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/move/container";
                }

                #endregion

                #region Headers

                headers = Common.AddToDictionary(CurrentSettings.Server.HeaderApiKey, CurrentSettings.Server.AdminApiKey, null);

                #endregion

                #region Process

                resp = RestRequest.SendRequestSafe(
                    url,
                    "application/json",
                    "POST",
                    null, null, false,
                    Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                    headers,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(currMove)));

                if (resp == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ContainerMoveReplica null REST response while writing to " + url);
                    return false;
                }

                if (resp.StatusCode != 200 && resp.StatusCode != 201)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ContainerMoveReplica non-200/201 REST response while writing to " + url);
                    return false;
                }

                Logging.Log(LoggingModule.Severity.Debug, "ContainerMoveReplica successfully replicated move operation to " + url);
                return true;

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ContainerMoveReplica", "Outer exception", e);
                return false;
            }
        }

        private void ContainerRenameReplicaAsync(RenameRequest currRename, Node currNode)
        {
            try
            {
                #region Check-for-Null-Values

                if (currRename == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerRenameReplicaAsync null value for currRename");
                return;
            }

            if (currNode == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerRenameReplicaAsync null value for currNode");
                return;
            }

            #endregion

                #region Process

                if (!ContainerRenameReplica(currRename, currNode))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ContainerRenameReplicaAsync unable to replicate rename operation to node ID " + currNode.NodeId + " " + currNode.Name);
                    SendReplica(currNode, "POST /admin/replication/rename/container", Common.SerializeJson(currRename));
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ContainerRenameReplicaAsync successfully replicated rename operation to node ID " + currNode.NodeId + " " + currNode.Name);
                }

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ContainerRenameReplicaAsync", "Outer exception", e);
                return;
            }
        }

        private bool ContainerRenameReplica(RenameRequest currRename, Node currNode)
        {
            try
            {
                #region Check-for-Null-Values

                if (currRename == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerWriteReplica null value for currRename");
                return false;
            }

            if (currNode == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerWriteReplica null value for currNode");
                return false;
            }

            #endregion

                #region Variables

                Dictionary<string, string> headers = new Dictionary<string, string>();
                RestResponse resp = new RestResponse();
                string url = "";

                #endregion

                #region Generate-URL

                if (Common.IsTrue(currNode.Ssl))
                {
                    url = "https://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/rename/container";
                }
                else
                {
                    url = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/rename/container";
                }

                #endregion

                #region Headers

                headers = Common.AddToDictionary(CurrentSettings.Server.HeaderApiKey, CurrentSettings.Server.AdminApiKey, null);

                #endregion

                #region Process

                resp = RestRequest.SendRequestSafe(
                    url,
                    "application/json",
                    "POST",
                    null, null, false,
                    Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                    headers,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(currRename)));

                if (resp == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ContainerRenameReplica null REST response while writing to " + url);
                    return false;
                }

                if (resp.StatusCode != 200 && resp.StatusCode != 201)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ContainerRenameReplica non-200/201 REST response while writing to " + url);
                    return false;
                }

                Logging.Log(LoggingModule.Severity.Debug, "ContainerRenameReplica successfully replicated rename operation to " + url);
                return true;

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ContainerRenameReplica", "Outer exception", e);
                return false;
            }
        }

        private void ContainerWriteReplicaAsync(Obj currObj)
        {
            try
            {
                #region Check-for-Null-Values

                if (currObj == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerWriteReplicaAsync null value for currObj");
                return;
            }

            if (currObj.Replicas == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerWriteReplicaAsync null value for currObj replicas");
                return;
            }

            if (currObj.Replicas.Count < 1)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerWriteReplicaAsync empty list for replicas");
                return;
            }

            #endregion

                #region Process

                foreach (Node curr in currObj.Replicas)
                {
                    if (!ContainerWriteReplica(currObj, curr))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ContainerWriteReplicaAsync unable to replicate container from " + currObj.DiskPath + " to node ID " + curr.NodeId + " " + curr.Name);
                        SendReplica(curr, "POST /admin/replication/container", Common.SerializeJson(currObj));
                    }
                    else
                    {
                        Logging.Log(LoggingModule.Severity.Debug, "ContainerWriteReplicaAsync successfully replicated container to node ID " + curr.NodeId + " " + curr.Name);
                    }
                }

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ContainerWriteReplicaAsync", "Outer exception", e);
                return;
            }
        }

        private bool ContainerWriteReplica(Obj currObj, Node currNode)
        {
            try
            {
                #region Check-for-Null-Values

                if (currObj == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerWriteReplica null value for currObj");
                return false;
            }

            if (currNode == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ContainerWriteReplica null value for currNode");
                return false;
            }

            #endregion

                #region Variables

                Dictionary<string, string> headers = new Dictionary<string, string>();
                RestResponse resp = new RestResponse();
                string url = "";

                #endregion

                #region Generate-URL

                if (Common.IsTrue(currNode.Ssl))
                {
                    url = "https://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/container";
                }
                else
                {
                    url = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/container";
                }

                #endregion

                #region Headers

                headers = Common.AddToDictionary(CurrentSettings.Server.HeaderApiKey, CurrentSettings.Server.AdminApiKey, null);

                #endregion

                #region Process

                resp = RestRequest.SendRequestSafe(
                    url,
                    "application/json",
                    "POST",
                    null, null, false,
                    Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                    headers,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(currObj)));

                if (resp == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ContainerWriteReplica null REST response while writing to " + url);
                    return false;
                }

                if (resp.StatusCode != 200 && resp.StatusCode != 201)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ContainerWriteReplica non-200/201 REST response while writing to " + url);
                    return false;
                }

                Logging.Log(LoggingModule.Severity.Debug, "ContainerWriteReplica successfully replicated container to " + url);
                return true;

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ContainerWriteReplica", "Outer exception", e);
                return false;
            }
        }

        private void ObjectMoveReplicaAsync(MoveRequest currMove, Node currNode)
        {
            try
            {
                #region Check-for-Null-Values

                if (currMove == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectMoveReplicaAsync null value for currMove");
                return;
            }

            if (currNode == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectMoveReplicaAsync null value for currNode");
                return;
            }

            #endregion

                #region Process

                if (!ObjectMoveReplica(currMove, currNode))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ObjectMoveReplicaAsync unable to replicate move operation to node ID " + currNode.NodeId + " " + currNode.Name);
                    SendReplica(currNode, "POST /admin/replication/move/object", Common.SerializeJson(currMove));
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ObjectMoveReplicaAsync successfully replicated move operation to node ID " + currNode.NodeId + " " + currNode.Name);
                }

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ObjectMoveReplicaAsync", "Outer exception", e);
                return;
            }
        }

        private bool ObjectMoveReplica(MoveRequest currMove, Node currNode)
        {
            try
            {
                #region Check-for-Null-Values

                if (currMove == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectMoveReplica null value for currMove");
                return false;
            }

            if (currNode == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectMoveReplica null value for currNode");
                return false;
            }

            #endregion

                #region Variables

                Dictionary<string, string> headers = new Dictionary<string, string>();
                RestResponse resp = new RestResponse();
                string url = "";

                #endregion

                #region Generate-URL

                if (Common.IsTrue(currNode.Ssl))
                {
                    url = "https://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/move/object";
                }
                else
                {
                    url = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/move/object";
                }

                #endregion

                #region Headers

                headers = Common.AddToDictionary(CurrentSettings.Server.HeaderApiKey, CurrentSettings.Server.AdminApiKey, null);

                #endregion

                #region Process

                resp = RestRequest.SendRequestSafe(
                    url,
                    "application/json",
                    "POST",
                    null, null, false,
                    Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                    headers,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(currMove)));

                if (resp == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ObjectMoveReplica null REST response while writing to " + url);
                    return false;
                }

                if (resp.StatusCode != 200 && resp.StatusCode != 201)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ObjectMoveReplica non-200/201 REST response while writing to " + url);
                    return false;
                }

                Logging.Log(LoggingModule.Severity.Debug, "ObjectMoveReplica successfully replicated move operation to " + url);
                return true;

                    #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ObjectMoveReplica", "Outer exception", e);
                return false;
            }
        }

        private void ObjectRenameReplicaAsync(RenameRequest currRename, Node currNode)
        {
            try
            {
                #region Check-for-Null-Values

                if (currRename == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectRenameReplicaAsync null value for currRename");
                return;
            }

            #endregion

                #region Process

                if (!ObjectRenameReplica(currRename, currNode))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ObjectRenameReplicaAsync unable to replicate rename operation to node ID " + currNode.NodeId + " " + currNode.Name);
                    SendReplica(currNode, "POST /admin/replication/rename/object", Common.SerializeJson(currRename));
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ObjectRenameReplicaAsync successfully replicated rename operation to node ID " + currNode.NodeId + " " + currNode.Name);
                }

                #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ObjectRenameReplicaAsync", "Outer exception", e);
                return;
            }
        }

        private bool ObjectRenameReplica(RenameRequest currRename, Node currNode)
        {
            try
            {
                #region Check-for-Null-Values

                if (currRename == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectRenameReplica null value for currRename");
                return false;
            }

            if (currNode == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectRenameReplica null value for currNode");
                return false;
            }

            #endregion

                #region Variables

                Dictionary<string, string> headers = new Dictionary<string, string>();
                RestResponse resp = new RestResponse();
                string url = "";

                #endregion

                #region Generate-URL

                if (Common.IsTrue(currNode.Ssl))
                {
                    url = "https://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/rename/object";
                }
                else
                {
                    url = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/rename/object";
                }

                #endregion

                #region Headers

                headers = Common.AddToDictionary(CurrentSettings.Server.HeaderApiKey, CurrentSettings.Server.AdminApiKey, null);

                #endregion

                #region Process

                resp = RestRequest.SendRequestSafe(
                    url,
                    "application/json",
                    "POST",
                    null, null, false,
                    Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                    headers,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(currRename)));

                if (resp == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ObjectRenameReplica null REST response while writing to " + url);
                    return false;
                }

                if (resp.StatusCode != 200 && resp.StatusCode != 201)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ObjectRenameReplica non-200/201 REST response while writing to " + url);
                    return false;
                }

                Logging.Log(LoggingModule.Severity.Debug, "ObjectRenameReplica successfully replicated rename operation to " + url);
                return true;

                #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ObjectRenameReplica", "Outer exception", e);
                return false;
            }
        }

        private void ObjectWriteReplicaAsync(Obj currObj)
        {
            try
            {
                #region Check-for-Null-Values

                if (currObj == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteReplicaAsync null value for currObj");
                return;
            }

            if (currObj.Replicas == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteReplicaAsync null value for replicas");
                return;
            }

            if (currObj.Replicas.Count < 1)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteReplicaAsync empty list for replicas");
                return;
            }

            #endregion

                #region Process

                foreach (Node currNode in currObj.Replicas)
                {
                    if (!ObjectWriteReplica(currObj, currNode))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteReplicaAsync unable to replicate object " + currObj.Key + " to node ID " + currNode.NodeId + " " + currNode.Name);
                        SendReplica(currNode, "POST /admin/replication/object", Common.SerializeJson(currObj));
                    }
                    else
                    {
                        Logging.Log(LoggingModule.Severity.Debug, "ObjectWriteReplicaAsync successfully replicated object " + currObj.Key + " to node ID " + currNode.NodeId + " " + currNode.Name);
                    }
                }

                #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ObjectWriteReplicaAsync", "Outer exception", e);
                return;
            }
        }

        private bool ObjectWriteReplica(Obj currObj, Node currNode)
        {
            try
            {
                #region Check-for-Null-Values

                if (currObj == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteReplica null value for currObj");
                return false;
            }

            if (currNode == null)
            {
                Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteReplica null value for currNode");
                return false;
            }

            #endregion

                #region Variables

                Dictionary<string, string> headers = new Dictionary<string, string>();
                RestResponse resp = new RestResponse();
                string url = "";

                #endregion

                #region Generate-URL

                if (Common.IsTrue(currNode.Ssl))
                {
                    url = "https://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/object";
                }
                else
                {
                    url = "http://" + currNode.DnsHostname + ":" + currNode.Port + "/admin/replication/object";
                }

                #endregion

                #region Headers

                headers = Common.AddToDictionary(CurrentSettings.Server.HeaderApiKey, CurrentSettings.Server.AdminApiKey, null);

                #endregion

                #region Process

                resp = RestRequest.SendRequestSafe(
                    url,
                    "application/json",
                    "POST",
                    null, null, false,
                    Common.IsTrue(CurrentSettings.Rest.AcceptInvalidCerts),
                    headers,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(currObj)));

                if (resp == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteReplica null REST response while writing to " + url);
                    return false;
                }

                if (resp.StatusCode != 200 && resp.StatusCode != 201)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "ObjectWriteReplica non-200/201 REST response while writing to " + url);
                    return false;
                }

                Logging.Log(LoggingModule.Severity.Debug, "ObjectWriteReplica successfully replicated " + currObj.Key + " to " + url);
                return true;

                #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("ObjectWriteReplica", "Outer exception", e);
                return false;
            }
        }

        #endregion

        #region Server-Receive-Methods

        #endregion

        #region General-Methods

        private bool RewriteTree(string root)
        {
            try
            { 
                #region Check-for-Null-Values

                if (String.IsNullOrEmpty(root))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "RewriteTree null root directory supplied");
                    return false;
                }

                #endregion

                #region Variables

                List<string> dirlist = new List<string>();
                List<string> filelist = new List<string>();
                long byteCount = 0;

                #endregion

                #region Get-Full-File-List

                if (!Common.WalkDirectory(CurrentSettings.Environment, 0, root, true, out dirlist, out filelist, out byteCount, true))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "RewriteTree unable to walk directory for " + root);
                    return false;
                }

                #endregion

                #region Process-Each-File

                foreach (string currFile in filelist)
                {
                    if (!RewriteObject(currFile))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "RewriteTree unable to rewrite file " + currFile);
                    }
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                Logging.Exception("RewriteTree", "Outer exception", e);
                return false;
            }
        }

        private bool RewriteObject(string filename)
        {
            try
            { 
                #region Check-for-Null-Values

                if (String.IsNullOrEmpty(filename))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "RewriteObject null filename supplied");
                    return false;
                }

                #endregion

                #region Variables

                Obj currObj = new Obj();
                List<string> containers = new List<string>();
                string random = "";
                bool writeSuccess = false;

                #endregion

                #region Retrieve-Object

                currObj = Obj.BuildObjFromDisk(filename, Users, CurrentSettings, CurrentTopology, CurrentNode, Logging);
                if (currObj == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "RewriteObject unable to build disk obj from file " + filename);
                    return false;
                }

                #endregion

                #region Generate-Random-String

                random = Common.RandomString(8);

                #endregion

                #region Rename-Original

                if (!Common.RenameFile(filename, filename + "." + random))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "RewriteObject unable to rename " + filename + " to temporary filename " + filename + "." + random);
                    return false;
                }

                #endregion

                #region Delete-File

                if (!Common.DeleteFile(filename))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "RewriteObject unable to delete file " + filename);
                    return false;
                }

                #endregion

                #region Rewrite-File

                if (!Common.IsTrue(currObj.GatewayMode))
                {
                    writeSuccess = Common.WriteFile(filename, Common.SerializeJson(currObj), false);
                    if (!writeSuccess)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "RewriteObject unable to write object to " + filename);
                        return false;
                    }
                }
                else
                {
                    writeSuccess = Common.WriteFile(filename, currObj.Value);
                    if (!writeSuccess)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "RewriteObject unable to write raw bytes to " + filename);
                        return false;
                    }
                }

                #endregion

                #region Delete-Temporary-File-and-Return

                if (!Common.DeleteFile(filename + "." + random))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "RewriteObject " + filename + " was successfully rerwritten but temporary file " + filename + "." + random + " was unable to be deleted");
                    return true;
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Debug, "RewriteObject successfully rewrote object " + filename);
                    return true;
                }

                #endregion
            }
            catch (Exception e)
            {
                Logging.Exception("RewriteObject", "Outer exception", e);
                return false;
            }
        }

        #endregion

        #endregion
    }
}
