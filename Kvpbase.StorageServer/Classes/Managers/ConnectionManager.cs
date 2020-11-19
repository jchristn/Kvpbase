using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using SyslogLogging;
using WatsonWebserver;
using Kvpbase.StorageServer.Classes.DatabaseObjects;

namespace Kvpbase.StorageServer.Classes.Managers
{
    internal class ConnectionManager
    { 
        private List<Connection> _Connections;
        private readonly object _ConnectionsLock;

        internal ConnectionManager()
        {
            _Connections = new List<Connection>();
            _ConnectionsLock = new object();
        }

        internal void Add(int threadId, HttpContext ctx)
        {
            if (threadId <= 0) return;
            if (ctx == null) return;
            if (ctx.Request == null) return;

            Connection conn = new Connection();
            conn.ThreadId = threadId;
            conn.SourceIp = ctx.Request.Source.IpAddress;
            conn.SourcePort = ctx.Request.Source.Port;
            conn.UserGUID = null; 
            conn.Method = ctx.Request.Method;
            conn.RawUrl = ctx.Request.Url.RawWithoutQuery;
            conn.StartTime = DateTime.Now;
            conn.EndTime = DateTime.Now;

            lock (_ConnectionsLock)
            {
                _Connections.Add(conn);
            }
        }

        internal void Close(int threadId)
        {
            if (threadId <= 0) return;

            lock (_ConnectionsLock)
            {
                _Connections = _Connections.Where(x => x.ThreadId != threadId).ToList();
            }
        }

        internal void Update(int threadId, UserMaster user)
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
                        curr.UserGUID = user.GUID;
                        tempList.Add(curr);
                    }
                }

                _Connections = tempList;
            }
        }

        internal List<Connection> GetActiveConnections()
        {
            List<Connection> curr = new List<Connection>();

            lock (_ConnectionsLock)
            {
                curr = new List<Connection>(_Connections);
            }

            return curr;
        } 
    }
}
