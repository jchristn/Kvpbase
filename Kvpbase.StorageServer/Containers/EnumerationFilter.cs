using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Kvpbase.Classes;

namespace Kvpbase.Containers
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
        /// If not null, match objects with the specified prefix in the key.
        /// </summary>
        public string Prefix { get; set; }

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

        /// <summary>
        /// If not null, match objects that contain each of the supplied key-value pairs.
        /// </summary>
        public Dictionary<string, string> KeyValuePairs { get; set; }

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
            Tags = new List<string>();
            KeyValuePairs = new Dictionary<string, string>();
        }

        /// <summary>
        /// Create an EnumerationFilter from RequestMetadata.
        /// </summary>
        /// <param name="md">RequestMetadata.</param>
        /// <returns>EnumerationFilter.</returns>
        public static EnumerationFilter FromRequestMetadata(RequestMetadata md)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));

            EnumerationFilter ret = new EnumerationFilter();
             
            if (md.Params != null)
            {
                if (!String.IsNullOrEmpty(md.Params.Prefix)) ret.Prefix = md.Params.Prefix;
                ret.CreatedAfter = md.Params.CreatedAfter;
                ret.CreatedBefore = md.Params.CreatedBefore;
                ret.LastAccessAfter = md.Params.LastAccessAfter;
                ret.LastAccessBefore = md.Params.LastAccessBefore;
                ret.UpdatedAfter = md.Params.UpdatedAfter;
                ret.UpdatedBefore = md.Params.UpdatedBefore;
                ret.Md5 = md.Params.Md5;
                ret.SizeMax = md.Params.SizeMax;
                ret.SizeMin = md.Params.SizeMin;
                if (!String.IsNullOrEmpty(md.Params.Tags)) ret.Tags = Common.CsvToStringList(md.Params.Tags); 
            }  

            return ret;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Create a human-readable string of the object.
        /// </summary>
        /// <returns>String.</returns>
        public override string ToString()
        {
            string ret = "";

            ret += "---" + Environment.NewLine;
            ret += "  Prefix       : " + (String.IsNullOrEmpty(Prefix) ? "null" : Prefix) + Environment.NewLine;
            ret += "  Md5          : " + (String.IsNullOrEmpty(Md5) ? "null" : Md5) + Environment.NewLine;
            ret += "  Content Type : " + (String.IsNullOrEmpty(ContentType) ? "null" : ContentType) + Environment.NewLine;
            ret += "  Created" + Environment.NewLine;
            ret += "    Before     : " + (CreatedBefore == null ? "null" : CreatedBefore.Value.ToString()) + Environment.NewLine;
            ret += "    After      : " + (CreatedAfter == null ? "null" : CreatedAfter.Value.ToString()) + Environment.NewLine;
            ret += "  Updated" + Environment.NewLine;
            ret += "    Before     : " + (UpdatedBefore == null ? "null" : UpdatedBefore.Value.ToString()) + Environment.NewLine;
            ret += "    After      : " + (UpdatedAfter == null ? "null" : UpdatedAfter.Value.ToString()) + Environment.NewLine;
            ret += "  Last Access" + Environment.NewLine;
            ret += "    Before     : " + (LastAccessBefore == null ? "null" : LastAccessBefore.Value.ToString()) + Environment.NewLine;
            ret += "    After      : " + (LastAccessAfter == null ? "null" : LastAccessAfter.Value.ToString()) + Environment.NewLine;
            ret += "  Size         " + Environment.NewLine;
            ret += "    Min        : " + (SizeMin == null ? "null" : SizeMin.ToString()) + Environment.NewLine;
            ret += "    After      : " + (SizeMax == null ? "null" : SizeMax.ToString()) + Environment.NewLine;
            ret += "  Tags         : " + ((Tags == null || Tags.Count < 1) ? "null" : Tags.Count.ToString() + " tags") + Environment.NewLine;

            if (Tags != null && Tags.Count > 0)
                foreach (string curr in Tags)
                    Console.WriteLine("    " + curr + Environment.NewLine);

            ret += "  Key Values   : " + ((KeyValuePairs == null || KeyValuePairs.Count < 1) ? "null" : KeyValuePairs.Count + " pairs") + Environment.NewLine;

            if (KeyValuePairs != null && KeyValuePairs.Count > 0)
                foreach (KeyValuePair<string, string> curr in KeyValuePairs)
                    Console.WriteLine("    " + curr.Key + " = " + curr.Value + Environment.NewLine);

            return ret; 
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
