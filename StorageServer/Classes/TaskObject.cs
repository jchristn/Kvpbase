using System;
using SyslogLogging;

namespace Kvpbase
{
    public class TaskObject
    {
        #region Public-Members

        public DateTime? Created { get; set; }
        public DateTime? Expiration { get; set;  }
        public DateTime? Completion { get; set; }
        public string TaskType { get; set; }
        public string Owner { get; set; }
        public string Description { get; set; }
        public object Data { get; set; }

        #endregion

        #region Constructors-and-Factories

        public TaskObject()
        {

        }

        #endregion
    }
}
