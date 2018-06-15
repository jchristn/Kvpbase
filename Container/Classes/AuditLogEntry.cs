using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kvpbase
{
    /// <summary>
    /// Details about an audit log entry.
    /// </summary>
    public class AuditLogEntry
    {
        #region Public-Members
         
        /// <summary>
        /// ID of the entry.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Key, if any, associated with the action.
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// Action performed by the requestor.
        /// </summary>
        public string Action { get; set; }

        /// <summary>
        /// Metadata associated with the action.
        /// </summary>
        public string Metadata { get; set; }

        /// <summary>
        /// Timestamp of the action.
        /// </summary>
        public DateTime? CreatedUtc { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public AuditLogEntry()
        {

        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Create an AuditLogEntry from a DataRow.
        /// </summary>
        /// <param name="row">DataRow.</param>
        /// <returns>AuditLogEntry.</returns>
        public static AuditLogEntry FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));
            AuditLogEntry ret = new AuditLogEntry();
            ret.Id = Convert.ToInt32(row["Id"]);
            ret.Key = row["Key"].ToString();
            ret.Action = row["Action"].ToString();
            ret.Metadata = row["Metadata"].ToString();
            ret.CreatedUtc = Convert.ToDateTime(row["CreatedUtc"]);
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
