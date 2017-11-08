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

        private Events _Logging { get; set; }
        private bool _Enabled { get; set; }

        #endregion

        #region Constructors-and-Factories

        public MaintenanceManager(Events logging)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            _Logging = logging;
            _Enabled = false;
        }

        #endregion

        #region Public-Methods

        public bool IsEnabled()
        {
            return _Enabled;
        }

        public void Set()
        {
            _Enabled = true;
            return;
        }

        public void Stop()
        {
            _Enabled = false;
            return;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
