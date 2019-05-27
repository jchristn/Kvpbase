using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kvpbase.Container
{
    /// <summary>
    /// Metadata for a container.
    /// </summary>
    public class ContainerMetadata
    {
        #region Public-Members

        /// <summary>
        /// The user that owns the container.
        /// </summary>
        public string User { get; set; }

        /// <summary>
        /// The name of the container.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Indicates whether or not public users can read from the container.
        /// </summary>
        public bool PublicRead { get; set; }

        /// <summary>
        /// Indicates whether or not public users can write to the container.
        /// </summary>
        public bool PublicWrite { get; set; }

        /// <summary>
        /// The number of objects in the container.
        /// </summary>
        public long TotalCount { get; set; }

        /// <summary>
        /// The number of objects in the response.
        /// </summary>
        public long Count { get; set; }

        /// <summary>
        /// The number of bytes consumed by objects in the container.
        /// </summary>
        public long TotalBytes { get; set; }

        /// <summary>
        /// The number of bytes consumed by objects listed in the results.
        /// </summary>
        public long Bytes { get; set; }

        /// <summary>
        /// The timestamp of the latest entry in the container.
        /// </summary>
        public DateTime? LatestEntry { get; set; }

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

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories
         
        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public ContainerMetadata()
        {

        }
         
        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods
         
        #endregion
    }
}
