using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Classes.Handlers;
using Kvpbase.Container;


namespace Kvpbase.Classes.Messaging
{
    /// <summary>
    /// Handles inbound message requests.
    /// </summary>
    public class InboundMessageHandler
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

        public InboundMessageHandler(
            Settings settings, 
            LoggingModule logging, 
            ContainerHandler containerHandler,
            ObjectHandler objectHandler)
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

        public void ContainerList(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            List<ContainerSettings> containers = new List<ContainerSettings>();
            bool success = StorageServer.TcpGetContainerList(md, out containers);
            if (success)
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, true,
                    Encoding.UTF8.GetBytes(Common.SerializeJson(containers, false))); 
            }
            else
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, false, null);
            } 
        }

        public void ContainerEnumerate(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);

            ContainerMetadata ret = null;
            bool success = StorageServer.TcpGetContainer(md, out ret);
            if (success)
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, true, Encoding.UTF8.GetBytes(Common.SerializeJson(ret, false)));
            }
            else
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, false, null);
            } 
        }

        public void ContainerExists(Message msgIn, out Message msgOut)
        { 
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpHeadContainer(md);
            if (success)
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, true, Encoding.UTF8.GetBytes(success.ToString()));
            }
            else
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, false, null);
            } 
        }

        public void ContainerDelete(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpDeleteContainer(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, null); 
        }

        public void ContainerCreate(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpPostContainer(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, null); 
        }

        public void ContainerClearAuditLog(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpDeleteContainer(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, null);  
        }

        public void ContainerUpdate(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpPutContainer(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, null); 
        }

        public void ObjectExists(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpHeadObject(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, null); 
        }

        public void ObjectMetadata(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);

            ObjectMetadata metadata = null;
            byte[] data = null;
            bool success = StorageServer.TcpGetObject(md, out metadata, out data);

            if (metadata != null)
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, true, Encoding.UTF8.GetBytes(Common.SerializeJson(metadata, false)));
            }
            else
            {
                msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, false, null);
            } 
        }

        public void ObjectDelete(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpDeleteObject(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, null); 
        }

        public void ObjectCreate(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpPostObject(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, null); 
        }

        public void ObjectRead(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);

            ObjectMetadata metadata = null;
            byte[] data = null;
            bool success = StorageServer.TcpGetObject(md, out metadata, out data);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, data); 
        }

        public void ObjectRename(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpPutObject(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, null); 
        }

        public void ObjectWriteRange(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpPutObject(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, null); 
        }

        public void ObjectWriteTags(Message msgIn, out Message msgOut)
        {
            if (msgIn == null) throw new ArgumentNullException(nameof(msgIn));
            RequestMetadata md = Common.DeserializeJson<RequestMetadata>(msgIn.Data);
            bool success = StorageServer.TcpPutObject(md);
            msgOut = new Message(msgIn.To, msgIn.From, msgIn.Metadata, msgIn.Type, success, null);
        }

        #endregion
         
        #region Private-Methods

        #endregion
    }
}
