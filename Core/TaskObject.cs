using System; 

namespace Kvpbase
{
    /// <summary>
    /// A queued background task to be performed by the node.
    /// </summary>
    public class TaskObject
    {
        #region Public-Members

        /// <summary>
        /// The time at which the task was created.
        /// </summary>
        public DateTime? Created { get; set; }

        /// <summary>
        /// The time at which the task will expire and will be discard.
        /// </summary>
        public DateTime? Expiration { get; set; }

        /// <summary>
        /// The time at which the task completed.
        /// </summary>
        public DateTime? Completion { get; set; }

        /// <summary>
        /// The type of task.
        /// </summary>
        public TaskType Type { get; set; } 

        /// <summary>
        /// The ID of the node to which the task is involved.
        /// </summary>
        public int RecipientNodeId { get; set; }
        
        /// <summary>
        /// Data associated with the task.
        /// </summary>
        public object Data { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public TaskObject()
        {

        }

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        /// <param name="taskType">The type of task.</param>
        /// <param name="recipientNodeId">The ID of the node to which the task is involved.</param>
        /// <param name="data">Data associated with the task.</param>
        /// <param name="expirationSeconds">The number of seconds for which the task should be considered valid.</param>
        public TaskObject(TaskType taskType, int recipientNodeId, object data, int? expirationSeconds)
        {
            if (recipientNodeId < 0) throw new ArgumentException("Invalid node ID.");
            RecipientNodeId = recipientNodeId;
            Type = taskType;
            Data = data;

            DateTime ts = DateTime.Now.ToUniversalTime();
            Created = ts;

            if (expirationSeconds != null && expirationSeconds > 0) Expiration = ts.AddSeconds(Convert.ToInt32(expirationSeconds));
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
