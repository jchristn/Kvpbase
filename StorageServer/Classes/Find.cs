using System;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

namespace Kvpbase
{
    public class Find
    {
        #region Public-Members

        public string UserGuid { get; set; }
        public string Key { get; set; }
        public bool QueryTopology { get; set; }
        public bool Recursive { get; set; }
        public List<string> ContainerPath { get; set; }
        public List<SearchFilter> Filters { get; set; }
        public List<string> Urls { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public Find()
        {

        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
