using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase.Classes.Managers
{
    public class ConnectionManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private List<Connection> _Connections;
        private readonly object _ConnectionsLock;

        #endregion

        #region Constructors-and-Factories

        public ConnectionManager()
        {
            _Connections = new List<Connection>();
            _ConnectionsLock = new object();
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

            lock (_ConnectionsLock)
            {
                _Connections.Add(conn);
            }
        }

        public void Close(int threadId)
        {
            if (threadId <= 0) return;

            lock (_ConnectionsLock)
            {
                _Connections = _Connections.Where(x => x.ThreadId != threadId).ToList();
            }
        }

        public void Update(int threadId, UserMaster user)
        {
            if (threadId <= 0) return;
            if (user == null) return;

            List<Connection> tempList = new List<Connection>();

            lock (_ConnectionsLock)
            {
                foreach (Connection curr in _Connections)
                {
                    if (curr.ThreadId != threadId)
                    {
                        tempList.Add(curr);
                    }
                    else
                    {
                        curr.UserMasterId = user.UserMasterId;
                        curr.Email = user.Email;
                        tempList.Add(curr);
                    }
                }

                _Connections = tempList;
            }
        }

        public List<Connection> GetActiveConnections()
        {
            List<Connection> curr = new List<Connection>();

            lock (_ConnectionsLock)
            {
                curr = new List<Connection>(_Connections);
            }

            return curr;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
