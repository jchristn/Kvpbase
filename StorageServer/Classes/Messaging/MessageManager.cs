using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;

namespace Kvpbase.Classes.Messaging
{
    /// <summary>
    /// Processes messages received in TopologyManager callbacks.
    /// </summary>
    public class MessageManager
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private InboundMessageHandler _InboundMessageHandler;

        #endregion

        #region Constructors-and-Factories

        public MessageManager(Settings settings, LoggingModule logging, InboundMessageHandler inboundMessageHandler)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (inboundMessageHandler == null) throw new ArgumentNullException(nameof(inboundMessageHandler));

            _Settings = settings;
            _Logging = logging;
            _InboundMessageHandler = inboundMessageHandler;
        }

        #endregion

        #region Public-Methods

        public bool ProcessAsyncMessage(Message msg)
        {
            try
            {
                if (msg == null) throw new ArgumentNullException(nameof(msg));
                 
                // _Logging.Log(LoggingModule.Severity.Debug, "ProcessAsyncMessage " + msg.Type.ToString() + " received from node ID " + msg.From.NodeId);
                 
                Message resp = new Message(msg.To, msg.From, msg.Type, false, null);

                switch (msg.Type)
                {
                    case MessageType.Hello:
                        resp.Success = true;
                        break;

                    case MessageType.Echo:
                        resp.Success = true;
                        resp.Data = msg.Data;
                        break;

                    case MessageType.Console:
                        if (Environment.UserInteractive) Console.WriteLine(Encoding.UTF8.GetString(msg.Data));
                        resp.Success = true;
                        break;

                    case MessageType.Heartbeat:
                        resp.Success = true; 
                        break;

                    case MessageType.ContainerExists:
                        _InboundMessageHandler.ContainerExists(msg, out resp);
                        break;

                    case MessageType.ContainerList:
                        _InboundMessageHandler.ContainerList(msg, out resp);
                        break;

                    case MessageType.ContainerEnumerate:
                        _InboundMessageHandler.ContainerEnumerate(msg, out resp);
                        break;

                    case MessageType.ReplicationContainerCreate:
                        _InboundMessageHandler.ContainerCreate(msg, out resp);
                        break;

                    case MessageType.ReplicationContainerUpdate:
                        _InboundMessageHandler.ContainerUpdate(msg, out resp);
                        break;

                    case MessageType.ReplicationContainerClearAuditLog:
                        _InboundMessageHandler.ContainerClearAuditLog(msg, out resp);
                        break;

                    case MessageType.ReplicationContainerDelete:
                        _InboundMessageHandler.ContainerDelete(msg, out resp);
                        break;

                    case MessageType.ObjectExists:
                        _InboundMessageHandler.ObjectExists(msg, out resp);
                        break;

                    case MessageType.ObjectMetadata:
                        _InboundMessageHandler.ObjectMetadata(msg, out resp);
                        break;

                    case MessageType.ObjectRead:
                        _InboundMessageHandler.ObjectRead(msg, out resp);
                        break;

                    case MessageType.ReplicationObjectCreate:
                        _InboundMessageHandler.ObjectCreate(msg, out resp);
                        break;

                    case MessageType.ReplicationObjectWriteRange:
                        _InboundMessageHandler.ObjectWriteRange(msg, out resp);
                        break;

                    case MessageType.ReplicationObjectWriteTags:
                        _InboundMessageHandler.ObjectWriteTags(msg, out resp);
                        break;

                    case MessageType.ReplicationObjectDelete:
                        _InboundMessageHandler.ObjectDelete(msg, out resp);
                        break;

                    case MessageType.ReplicationObjectRename:
                        _InboundMessageHandler.ObjectRename(msg, out resp);
                        break;
                }

                return Common.IsTrue(resp.Success);
            }
            catch (Exception e)
            {
                _Logging.LogException("MessageManager", "ProcessAsyncMessage", e);
                return false;
            }
        }

        public Message ProcessSyncMessage(Message msg)
        {
            try
            { 
                if (msg == null) throw new ArgumentNullException(nameof(msg));
                 
                // _Logging.Log(LoggingModule.Severity.Debug, "ProcessSyncMessage " + msg.Type.ToString() + " received from node ID " + msg.From.NodeId);

                Message resp = new Message(msg.To, msg.From, msg.Type, false, null); 

                switch (msg.Type)
                {
                    case MessageType.Hello:
                        resp.Data = Encoding.UTF8.GetBytes("Hello");
                        resp.Success = true;
                        break;

                    case MessageType.Echo:
                        resp.Data = msg.Data;
                        resp.Success = true;
                        break;

                    case MessageType.Console:
                        if (Environment.UserInteractive) Console.WriteLine(Encoding.UTF8.GetString(msg.Data));
                        resp.Data = Encoding.UTF8.GetBytes("Received");
                        resp.Success = true;
                        break;

                    case MessageType.ContainerList:
                        _InboundMessageHandler.ContainerList(msg, out resp);
                        break;

                    case MessageType.ContainerEnumerate:
                        _InboundMessageHandler.ContainerEnumerate(msg, out resp);
                        break;

                    case MessageType.ContainerExists:
                        _InboundMessageHandler.ContainerExists(msg, out resp);
                        break;

                    case MessageType.ReplicationContainerCreate:
                        _InboundMessageHandler.ContainerCreate(msg, out resp);
                        break;

                    case MessageType.ReplicationContainerUpdate:
                        _InboundMessageHandler.ContainerUpdate(msg, out resp);
                        break;

                    case MessageType.ReplicationContainerDelete:
                        _InboundMessageHandler.ContainerDelete(msg, out resp);
                        break;

                    case MessageType.ReplicationContainerClearAuditLog:
                        _InboundMessageHandler.ContainerClearAuditLog(msg, out resp);
                        break;

                    case MessageType.ObjectRead:
                        _InboundMessageHandler.ObjectRead(msg, out resp);
                        break;

                    case MessageType.ObjectExists:
                        _InboundMessageHandler.ObjectExists(msg, out resp);
                        break;

                    case MessageType.ObjectMetadata:
                        _InboundMessageHandler.ObjectMetadata(msg, out resp);
                        break;

                    case MessageType.ReplicationObjectCreate:
                        _InboundMessageHandler.ObjectCreate(msg, out resp);
                        break;

                    case MessageType.ReplicationObjectWriteRange:
                        _InboundMessageHandler.ObjectWriteRange(msg, out resp);
                        break;

                    case MessageType.ReplicationObjectWriteTags:
                        _InboundMessageHandler.ObjectWriteTags(msg, out resp);
                        break;

                    case MessageType.ReplicationObjectDelete:
                        _InboundMessageHandler.ObjectDelete(msg, out resp);
                        break;

                    case MessageType.ReplicationObjectRename:
                        _InboundMessageHandler.ObjectRename(msg, out resp);
                        break;

                    default:
                        break;
                }
                 
                return resp;
            }
            catch (Exception e)
            {
                _Logging.LogException("MessageManager", "ProcessSyncMessage", e);
                return null;
            }
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
