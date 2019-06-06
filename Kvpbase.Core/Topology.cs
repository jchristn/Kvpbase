using System;
using System.Collections.Generic;
using System.IO; 

namespace Kvpbase.Core
{
    /// <summary>
    /// The topology containing metadata on all of the Kvpbase nodes in the network.
    /// </summary>
    public class Topology
    {
        #region Public-Members

        /// <summary>
        /// The ID of the node.
        /// </summary>
        public int? NodeId { get; set; }

        /// <summary>
        /// The list of all nodes in the network, including the current node.
        /// </summary>
        public List<Node> Nodes { get; set; }

        /// <summary>
        /// List of IDs of all replicas for the current node.
        /// </summary>
        public List<int> Replicas { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public Topology()
        {

        }
         
        #endregion

        #region Public-Methods
         
        #endregion

        #region Private-Methods

        #endregion
    }
}
