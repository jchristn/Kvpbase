using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using SyslogLogging;

using Kvpbase.Classes;
using Kvpbase.Classes.Handlers;
using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase.Classes.Messaging
{
    /// <summary>
    /// Inbound message handler.
    /// </summary>
    public class InboundHandler
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private Settings _Settings;
        private LoggingModule _Logging;
        private ContainerHandler _ContainerHandler;
        private ObjectHandler _ObjectHandler;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="settings">Settings.</param>
        /// <param name="logging">LoggingModule instance.</param>
        /// <param name="containerHandler">ContainerHandler instance.</param>
        /// <param name="objectHandler">ObjectHandler instance.</param>
        public InboundHandler(Settings settings, LoggingModule logging, ContainerHandler containerHandler, ObjectHandler objectHandler)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (containerHandler == null) throw new ArgumentNullException(nameof(containerHandler));
            if (objectHandler == null) throw new ArgumentNullException(nameof(objectHandler));

            _Settings = settings;
            _Logging = logging;
            _ContainerHandler = containerHandler;
            _ObjectHandler = objectHandler;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Process an async stream.
        /// </summary>
        /// <param name="msg">Incoming message.</param>
        /// <returns>True if successful.</returns>
        public bool ProcessAsyncStream(Message msg)
        { 
            if (msg == null) throw new ArgumentNullException(nameof(msg));
             
            if (_Settings.Topology.DebugMeshNetworking) 
                _Logging.Log(LoggingModule.Severity.Info, "ProcessAsyncStream from node ID " + msg.From.NodeId + ": " + msg.ContentLength + " bytes"); 

            if (_Settings.Topology.DebugMessages) 
                _Logging.Log(LoggingModule.Severity.Info, "ProcessAsyncStream received: " + Environment.NewLine + msg.ToString()); 
             
            Message resp = new Message(msg.To, msg.From, msg.Metadata, msg.Type, false, 0, null); 

            switch (msg.Type)
            {  
                case MessageType.Console:
                    if (Environment.UserInteractive)
                        Console.WriteLine(Encoding.UTF8.GetString(Common.StreamToBytes(msg.DataStream)));
                    resp.Success = true;
                    break;
                     
                case MessageType.ContainerExists:
                    ContainerExists(msg, out resp);
                    break;

                case MessageType.ContainerList:
                    ContainerList(msg, out resp);
                    break;

                case MessageType.ContainerEnumerate:
                    ContainerEnumerate(msg, out resp);
                    break;

                case MessageType.ContainerCreate:
                    ContainerCreate(msg, out resp);
                    break;

                case MessageType.ContainerUpdate:
                    ContainerUpdate(msg, out resp);
                    break;

                case MessageType.ContainerClearAuditLog:
                    ContainerClearAuditLog(msg, out resp);
                    break;

                case MessageType.ContainerDelete:
                    ContainerDelete(msg, out resp);
                    break;

                case MessageType.ObjectExists:
                    ObjectExists(msg, out resp);
                    break;

                case MessageType.ObjectMetadata:
                    ObjectMetadata(msg, out resp);
                    break;

                case MessageType.ObjectRead:
                    ObjectRead(msg, out resp);
                    break;

                case MessageType.ObjectCreate:
                    ObjectCreate(msg, out resp);
                    break;

                case MessageType.ObjectWriteRange:
                    ObjectWriteRange(msg, out resp);
                    break;

                case MessageType.ObjectWriteTags:
                    ObjectWriteTags(msg, out resp);
                    break;

                case MessageType.ObjectDelete:
                    ObjectDelete(msg, out resp);
                    break;

                case MessageType.ObjectRename:
                    ObjectRename(msg, out resp);
                    break;
            }

            return Common.IsTrue(resp.Success); 
        }

        /// <summary>
        /// Process a synchronous stream, where a response is expected.
        /// </summary>
        /// <param name="msg">Message.</param>
        /// <returns>Message.</returns>
        public Message ProcessSyncStream(Message msg)
        { 
            if (msg == null) throw new ArgumentNullException(nameof(msg));

            if (_Settings.Topology.DebugMeshNetworking)
                _Logging.Log(LoggingModule.Severity.Info, "ProcessSyncStream from node ID " + msg.From.NodeId + ": " + msg.ContentLength + " bytes");

            if (_Settings.Topology.DebugMessages)
                _Logging.Log(LoggingModule.Severity.Info, "ProcessSyncStream received: " + Environment.NewLine + msg.ToString());

            Message resp = new Message(msg.To, msg.From, msg.Metadata, msg.Type, false, 0, null);
            byte[] respBytes = null;

            switch (msg.Type)
            { 
                case MessageType.Console:
                    if (Environment.UserInteractive)
                        Console.WriteLine(Encoding.UTF8.GetString(Common.StreamToBytes(msg.DataStream)));
                    respBytes = Encoding.UTF8.GetBytes("Received");
                    resp.DataStream = new MemoryStream();
                    resp.DataStream.Write(respBytes, 0, respBytes.Length);
                    if (resp.DataStream.CanSeek) resp.DataStream.Seek(0, SeekOrigin.Begin);
                    resp.Success = true;
                    break;

                case MessageType.ContainerList: 
                    ContainerList(msg, out resp); 
                    break;

                case MessageType.ContainerEnumerate:
                    ContainerEnumerate(msg, out resp);
                    break;

                case MessageType.ContainerExists:
                    ContainerExists(msg, out resp);
                    break;

                case MessageType.ContainerCreate:
                    ContainerCreate(msg, out resp);
                    break;

                case MessageType.ContainerUpdate:
                    ContainerUpdate(msg, out resp);
                    break;

                case MessageType.ContainerDelete:
                    ContainerDelete(msg, out resp);
                    break;

                case MessageType.ContainerClearAuditLog:
                    ContainerClearAuditLog(msg, out resp);
                    break;

                case MessageType.ObjectRead:
                    ObjectRead(msg, out resp);
                    break;

                case MessageType.ObjectExists:
                    ObjectExists(msg, out resp);
                    break;

                case MessageType.ObjectMetadata:
                    ObjectMetadata(msg, out resp);
                    break;

                case MessageType.ObjectCreate:
                    ObjectCreate(msg, out resp);
                    break;

                case MessageType.ObjectWriteRange:
                    ObjectWriteRange(msg, out resp);
                    break;

                case MessageType.ObjectWriteTags:
                    ObjectWriteTags(msg, out resp);
                    break;

                case MessageType.ObjectDelete:
                    ObjectDelete(msg, out resp);
                    break;

                case MessageType.ObjectRename:
                    ObjectRename(msg, out resp);
                    break;

                default:
                    break;
            }

            if (resp != null)
            {
                if (_Settings.Topology.DebugMessages)
                    _Logging.Log(LoggingModule.Severity.Info, "ProcessSyncStream sending response: " + Environment.NewLine + resp.ToString());

                return resp;
            }
            else
            {
                if (_Settings.Topology.DebugMessages)
                    _Logging.Log(LoggingModule.Severity.Info, "ProcessSyncStream no response to send");

                return null;
            }
        }

        #endregion

        #region Private-Container-Methods

        private void ContainerCreate(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            bool success = StorageServer.TcpPostContainer(msgIn.Metadata);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, 0, null);
        }

        private void ContainerDelete(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            bool success = StorageServer.TcpDeleteContainer(msgIn.Metadata);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, 0, null);
        }

        private void ContainerUpdate(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            bool success = StorageServer.TcpPutContainer(msgIn.Metadata);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, 0, null);
        }

        private void ContainerClearAuditLog(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            bool success = StorageServer.TcpDeleteContainer(msgIn.Metadata);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, 0, null);
        }

        private void ContainerList(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            List<ContainerSettings> containers = new List<ContainerSettings>();
            bool success = StorageServer.TcpGetContainerList(msgIn.Metadata, out containers); 

            if (success)
            {
                MemoryStream ms = new MemoryStream();
                byte[] data = Encoding.UTF8.GetBytes(Common.SerializeJson(containers, false)); 
                ms.Write(data, 0, data.Length);
                ms.Seek(0, SeekOrigin.Begin);
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, true, data.Length, ms);
            }
            else
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, false, 0, null);
            }
        }

        private void ContainerEnumerate(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            ContainerMetadata ret = null;
            bool success = StorageServer.TcpGetContainer(msgIn.Metadata, out ret);
            if (success)
            {
                MemoryStream ms = new MemoryStream();
                byte[] data = Encoding.UTF8.GetBytes(Common.SerializeJson(ret, false));
                ms.Write(data, 0, data.Length);
                ms.Seek(0, SeekOrigin.Begin);
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, true, data.Length, ms);
            }
            else
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, false, 0, null);
            }
        }

        private void ContainerExists(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            bool success = StorageServer.TcpHeadContainer(msgIn.Metadata);
            if (success)
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, true, 0, null);
            }
            else
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, false, 0, null);
            }
        }

        #endregion

        #region Private-Object-Methods

        private void ObjectCreate(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = msgIn.Metadata;
            md.Http.DataStream = msgIn.DataStream;
            if (md.Http.DataStream.CanSeek) md.Http.DataStream.Seek(0, SeekOrigin.Begin);
            bool success = StorageServer.TcpPostObject(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, 0, null);
        }

        private void ObjectDelete(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            bool success = StorageServer.TcpDeleteObject(msgIn.Metadata);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, 0, null);
        }

        private void ObjectWriteRange(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            msgIn.Metadata.Http.DataStream = msgIn.DataStream;
            bool success = StorageServer.TcpPutObject(msgIn.Metadata);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, 0, null);
        }

        private void ObjectWriteTags(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            bool success = StorageServer.TcpPutObject(msgIn.Metadata);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, 0, null);
        }

        private void ObjectRename(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            bool success = StorageServer.TcpPutObject(msgIn.Metadata);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, 0, null);
        }

        private void ObjectRead(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            ObjectMetadata metadata = null;
            long contentLength = 0;
            Stream stream = null;
            bool success = StorageServer.TcpGetObject(msgIn.Metadata, out metadata, out contentLength, out stream);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, contentLength, stream);
        }

        private void ObjectExists(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            bool success = StorageServer.TcpHeadObject(msgIn.Metadata);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, 0, null);
        }

        private void ObjectMetadata(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            ObjectMetadata metadata = null;
            long contentLength = 0;
            Stream stream = null;
            bool success = StorageServer.TcpGetObject(msgIn.Metadata, out metadata, out contentLength, out stream);

            if (metadata != null)
            {
                MemoryStream ms = new MemoryStream();
                byte[] mdBytes = Encoding.UTF8.GetBytes(Common.SerializeJson(metadata, false));
                ms.Write(mdBytes, 0, mdBytes.Length);
                ms.Seek(0, SeekOrigin.Begin);
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, true, mdBytes.Length, ms);
            }
            else
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, false, 0, null);
            }
        }

        #endregion
    }
}
