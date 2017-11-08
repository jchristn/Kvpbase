using System;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

namespace Kvpbase
{
    public class Topology
    {
        #region Public-Members

        public DateTime? LastProcessed { get; set; }
        public int? CurrNodeId { get; set; }
        public List<Node> Nodes { get; set; }
        public List<Node> Replicas { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public Topology()
        {

        }

        public static Topology FromFile(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));
            if (!Common.FileExists(filename)) throw new FileNotFoundException(nameof(filename));

            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("Reading topoogy from " + filename);
            string contents = Common.ReadTextFile(@filename);

            if (String.IsNullOrEmpty(contents))
            {
                Common.ExitApplication("Topology", "Unable to read contents of " + filename, -1);
                return null;
            }

            Console.WriteLine("Deserializing " + filename);
            Topology ret = null;

            try
            {
                ret = Common.DeserializeJson<Topology>(contents);
                if (ret == null)
                {
                    Common.ExitApplication("Topology", "Unable to deserialize " + filename + " (null)", -1);
                    return null;
                }
            }
            catch (Exception e)
            {
                Events.ExceptionConsole("Topology", "Deserialization issue with " + filename, e);
                Common.ExitApplication("Topology", "Unable to deserialize " + filename + " (exception)", -1);
                return null;
            }

            return ret;
        }

        #endregion

        #region Public-Methods

        public bool PopulateReplicas(Node currentNode)
        {
            if (currentNode == null) throw new ArgumentNullException(nameof(currentNode));

            Replicas = new List<Node>();
            if (currentNode.Neighbors != null && currentNode.Neighbors.Count > 0)
            {
                Console.WriteLine("Topology has " + currentNode.Neighbors.Count + " defined");
                foreach (int currReplicaNodeId in currentNode.Neighbors)
                {
                    Console.WriteLine("  Evaluating node ID " + currReplicaNodeId);

                    foreach (Node curr in Nodes)
                    {
                        if (currReplicaNodeId == curr.NodeId)
                        {
                            Console.WriteLine("  Added node ID " + currReplicaNodeId + " as a replica");
                            Replicas.Add(curr);
                        }
                        else
                        {
                            Console.WriteLine("  Skipping node ID " + curr.NodeId + ", not a neighbor");
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("Topology has no neighbors defined");
            }

            Console.WriteLine("Topology validated without error (populated " + Replicas.Count + " replicas)");
            return true;
        }

        public bool ValidateTopology(out Node currentNode)
        {
            currentNode = null;
            List<int> allNodeIds = new List<int>();

            #region Build-All-Node-ID-List

            if (Nodes == null || Nodes.Count < 1)
            {
                Console.WriteLine("No nodes found in topology");
                return false;
            }

            foreach (Node curr in Nodes)
            {
                allNodeIds.Add(curr.NodeId);
            }

            #endregion

            #region Find-Current-Node

            bool currentNodeFound = false;

            foreach (Node curr in Nodes)
            {
                if (CurrNodeId == curr.NodeId)
                {
                    currentNode = curr;
                    currentNodeFound = true;
                    break;
                }
            }

            if (!currentNodeFound)
            {
                Console.WriteLine("Unable to find local node in topology");
                return false;
            }

            #endregion

            #region Verify-Replicas-Exit

            foreach (Node currNode in Nodes)
            {
                if (currNode.Neighbors == null || currNode.Neighbors.Count < 1) continue;

                foreach (int currReplicaNodeId in currNode.Neighbors)
                {
                    if (!allNodeIds.Contains(currReplicaNodeId))
                    {
                        Console.WriteLine("Replica node ID " + currReplicaNodeId + " not found in node list");
                        return false;
                    }
                }
            }

            #endregion

            return true;
        }

        public bool IsEmpty()
        {
            if (Nodes.Count < 2) return true;
            return false;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
