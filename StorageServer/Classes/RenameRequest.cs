using System;
using System.Collections.Generic;
using SyslogLogging;

namespace Kvpbase
{
    public class RenameRequest
    {
        #region Public-Members

        public string UserGuid;
        public List<string> ContainerPath;
        public string RenameFrom;
        public string RenameTo;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public RenameRequest()
        {

        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
