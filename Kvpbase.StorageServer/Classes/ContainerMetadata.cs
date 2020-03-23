using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Kvpbase.StorageServer.Classes;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer.Classes
{
    /// <summary>
    /// Metadata for a container.
    /// </summary>
    public class ContainerMetadata
    { 
        /// <summary>
        /// GUID for the container.
        /// </summary>
        public string ContainerGUID { get; set; }

        /// <summary>
        /// The name of the container.
        /// </summary>
        public string ContainerName { get; set; }

        /// <summary>
        /// The user GUID that owns the container.
        /// </summary>
        public string UserGUID { get; set; }

        /// <summary>
        /// Indicates whether or not public users can read from the container.
        /// </summary>
        public bool PublicRead { get; set; }

        /// <summary>
        /// Indicates whether or not public users can write to the container.
        /// </summary>
        public bool PublicWrite { get; set; }

        /// <summary>
        /// Total counts for the container.
        /// </summary>
        public Counts Totals { get; set; }

        /// <summary>
        /// Counts for objects returned in this query.
        /// </summary>
        public Counts Displayed { get; set; }
         
        /// <summary>
        /// The requested starting index, if any.
        /// </summary>
        public int? IndexStart { get; set; }

        /// <summary>
        /// The requested number of results to retrieve, if any.
        /// </summary>
        public int? MaxResults { get; set; }

        /// <summary>
        /// The requested enumeration filter, if any.
        /// </summary>
        public EnumerationFilter Filter { get; set; }

        /// <summary>
        /// List of object metadata.
        /// </summary>
        public List<ObjectMetadata> Objects { get; set; }
         
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public ContainerMetadata()
        {
            Totals = new Counts();
            Displayed = new Counts();
            Objects = new List<ObjectMetadata>();
        }
         
        /// <summary>
        /// Summary statistics.
        /// </summary>
        public class Counts
        {
            /// <summary>
            /// Number of objects.
            /// </summary>
            public long Objects { get; set; }

            /// <summary>
            /// Number of bytes.
            /// </summary>
            public long Bytes { get; set; }
        } 
    }
}
