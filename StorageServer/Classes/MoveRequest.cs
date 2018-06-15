using System;
using System.Collections.Generic;
using SyslogLogging;

namespace Kvpbase
{
    public class MoveRequest
    {
        #region Public-Members

        public string UserGuid;
        public List<string> FromContainer;
        public string MoveFrom;
        public List<string> ToContainer;
        public string MoveTo;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public MoveRequest()
        {

        }

        #endregion

        #region Public-Methods
         
        #endregion

        #region Private-Methods

        #endregion
    }
}
