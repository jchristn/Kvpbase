using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;

namespace Kvpbase
{
    public class MaintenanceManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Events Logging { get; set; }
        private bool Enabled { get; set; }

        #endregion

        #region Constructors-and-Factories

        public MaintenanceManager(Events logging)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            Logging = logging;
            Enabled = false;
        }

        #endregion

        #region Public-Methods

        public bool IsEnabled()
        {
            return Enabled;
        }

        public void Set()
        {
            Enabled = true;
            return;
        }

        public void Stop()
        {
            Enabled = false;
            return;
        }

        #endregion

        #region Public-Static-Methods

        #endregion
    }
}
