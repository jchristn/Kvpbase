using System;
using System.Collections.Generic;
using SyslogLogging;

namespace Kvpbase
{
    public class ObjectLogging
    {
        #region Public-Members

        public int? Enabled { get; set; }
        public int? ReadObject { get; set; }
        public int? CreateObject { get; set; }
        public int? DeleteObject { get; set; }

        #endregion

        #region Constructors-and-Factories

        public ObjectLogging()
        {

        }

        #endregion
    }
}
