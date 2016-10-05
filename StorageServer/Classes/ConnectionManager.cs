using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public class ConnectionManager
    {
        #region Private-Members

        private List<Connection> ActiveConnections;
        private readonly object ConnectionLock;

        #endregion

        #region Constructors-and-Factories

        public ConnectionManager()
        {
            ActiveConnections = new List<Connection>();
            ConnectionLock = new object();
        }

        #endregion

        #region Public-Methods

        public void Add(int threadId, HttpRequest req)
        {
            if (threadId <= 0) return;
            if (req == null) return;

            Connection conn = new Connection();
            conn.ThreadId = threadId;
            conn.SourceIp = req.SourceIp;
            conn.SourcePort = req.SourcePort;
            conn.UserMasterId = 0;
            conn.Email = "";
            conn.Method = req.Method;
            conn.RawUrl = req.RawUrlWithoutQuery;
            conn.StartTime = DateTime.Now;
            conn.EndTime = DateTime.Now;

            lock (ConnectionLock)
            {
                ActiveConnections.Add(conn);
            }
        }

        public void Close(int threadId)
        {
            if (threadId <= 0) return;

            lock (ConnectionLock)
            {
                ActiveConnections = ActiveConnections.Where(x => x.ThreadId != threadId).ToList();
            }
        }

        public void Update(int threadId, int userMasterId, string email)
        {
            if (threadId <= 0) return;
            if (userMasterId <= 0) return;
            if (String.IsNullOrEmpty(email)) return;

            List<Connection> tempList = new List<Connection>();

            lock (ConnectionLock)
            {
                foreach (Connection curr in ActiveConnections)
                {
                    if (curr.ThreadId != threadId)
                    {
                        tempList.Add(curr);
                    }
                    else
                    {
                        curr.UserMasterId = userMasterId;
                        curr.Email = email;
                        tempList.Add(curr);
                    }
                }

                ActiveConnections = tempList;
            }
        }

        public List<Connection> GetActiveConnections()
        {
            List<Connection> curr = new List<Connection>();

            lock (ConnectionLock)
            {
                curr = new List<Connection>(ActiveConnections);
            }

            return curr;
        }

        #endregion
    }
}
