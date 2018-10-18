using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kvpbase
{
    /// <summary>
    /// Filters to apply to container enumeration.
    /// </summary>
    public class EnumerationFilter
    {
        #region Public-Members
         
        /// <summary>
        /// If not null, match objects created before the specified time (UTC).
        /// </summary>
        public DateTime? CreatedBefore { get; set; }

        /// <summary>
        /// If not null, match objects created after the specified time (UTC).
        /// </summary>
        public DateTime? CreatedAfter { get; set; }

        /// <summary>
        /// If not null, match objects updated before the specified time (UTC).
        /// </summary>
        public DateTime? UpdatedBefore { get; set; }

        /// <summary>
        /// If not null, match objects updated after the specified time (UTC).
        /// </summary>
        public DateTime? UpdatedAfter { get; set; }

        /// <summary>
        /// If not null, match objects last accessed before the specified time (UTC).
        /// </summary>
        public DateTime? LastAccessBefore { get; set; }

        /// <summary>
        /// If not null, match objects last accessed after the specified time (UTC).
        /// </summary>
        public DateTime? LastAccessAfter { get; set; }

        /// <summary>
        /// If not null, match objects with the specified MD5 hash value.
        /// </summary>
        public string Md5 { get; set; }

        /// <summary>
        /// If not null, match objects of the specified content type.
        /// </summary>
        public string ContentType { get; set; }

        /// <summary>
        /// If not null, match objects of at least the specified size.
        /// </summary>
        public long? SizeMin { get; set; }

        /// <summary>
        /// If not null, match objects that are less than the specified size.
        /// </summary>
        public long? SizeMax { get; set; }

        /// <summary>
        /// If not null, match objects that contain each of the supplied tags.
        /// </summary>
        public List<string> Tags { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public EnumerationFilter()
        {
            CreatedBefore = null;
            CreatedAfter = null;
            UpdatedBefore = null;
            UpdatedAfter = null;
            LastAccessBefore = null;
            LastAccessAfter = null;
        }

        #endregion

        #region Public-Methods
         
        #endregion

        #region Private-Methods

        #endregion
    }
}
