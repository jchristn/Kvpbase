using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static bool TcpPutObject(RequestMetadata md)
        { 
            #region Retrieve-Container
             
            Container currContainer = null;
            if (!_ContainerMgr.GetContainer(md.Params.UserGuid, md.Params.Container, out currContainer))
            {
                _Logging.Warn("TcpPutObject unable to find container " + md.Params.UserGuid + "/" + md.Params.Container);
                return false;
            }
            
            #endregion
             
            #region Check-if-Object-Exists

            if (!currContainer.Exists(md.Params.ObjectKey))
            {
                _Logging.Warn("TcpPutObject object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " does not exists");
                return false;
            }

            #endregion
            
            #region Process

            ErrorCode error;
            if (!String.IsNullOrEmpty(md.Params.Rename))
            {
                #region Rename
                 
                if (!_ObjectHandler.Rename(md, currContainer, md.Params.ObjectKey, md.Params.Rename, out error))
                {
                    _Logging.Warn("TcpPutObject unable to rename object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + " to " + md.Params.Rename + ": " + error.ToString());
                    return false;
                }
                else
                { 
                    return true;
                }

                #endregion
            }
            else if (md.Params.Index != null)
            {
                #region Range-Write
                
                if (!_ObjectHandler.WriteRange(md, currContainer, md.Params.ObjectKey, Convert.ToInt64(md.Params.Index), md.Http.ContentLength, md.Http.DataStream, out error))
                {
                    _Logging.Warn("TcpPutObject unable to write range to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + ": " + error.ToString());
                    return false;
                }
                else
                { 
                    return true;
                }

                #endregion
            }
            else if (!String.IsNullOrEmpty(md.Params.Tags))
            {
                #region Tags
                  
                if (!currContainer.WriteObjectTags(md.Params.ObjectKey, md.Params.Tags, out error))
                {
                    _Logging.Warn("HttpPutObject unable to write tags to object " + md.Params.UserGuid + "/" + md.Params.Container + "/" + md.Params.ObjectKey + ": " + error.ToString());
                    return false;
                }
                else
                {
                    return true;
                }
                 
                #endregion
            }
            else
            {
                _Logging.Warn("TcpPutObject request query does not contain index start or rename"); 
                return false;
            }

            #endregion 
        }
    }
}