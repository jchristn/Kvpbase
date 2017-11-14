using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse PostMessage(RequestMetadata md)
        {
            #region Variables

            Message req = new Message();
            MoveRequest moveContainer = new MoveRequest();
            Obj delContainer = new Obj();
            Obj createContainer = new Obj();
            RenameRequest renameContainer = new RenameRequest();
            MoveRequest moveObject = new MoveRequest();
            Obj delObject = new Obj();
            Obj createObject = new Obj();
            RenameRequest renameObject = new RenameRequest(); 

            #endregion

            #region Check-for-Null-Values

            if (md.CurrHttpReq.Data == null || md.CurrHttpReq.Data.Length < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "PostMessage null request body, returning 400");
                return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                    new ErrorResponse(2, 400, "No request body.", null).ToJson(),
                    true);
            }
            
            #endregion

            #region Deserialize

            try
            {
                req = Common.DeserializeJson<Message>(md.CurrHttpReq.Data);
                if (req == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "PostMessage null request after deserialization, returning 400");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                        true);
                }
            }
            catch (Exception)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to deserialize request body");
                return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                    new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                    true);
            }

            #endregion

            #region Process

            switch (req.Subject)
            {
                case "DELETE /admin/replication/container":
                    #region delete_admin_replication_container
                        
                    try
                    {
                        delContainer = Common.DeserializeJson<Obj>(req.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage exception while deserializing message body for message of type DELETE /admin/replication/container, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                            true);
                    }

                    delContainer.DiskPath = _ObjMgr.DiskPath(delContainer, md.CurrUser);
                    if (String.IsNullOrEmpty(delContainer.DiskPath))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to build disk path for delete container operation");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(),
                            true);
                    }

                    if (Common.DirectoryExists(delContainer.DiskPath))
                    {
                        if (Common.DeleteDirectory(delContainer.DiskPath, true))
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "PostMessage successfully deleted " + delContainer.DiskPath);
                            return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                        }

                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to delete " + delContainer.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to delete container.", null).ToJson(),
                            true);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to find " + delContainer.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                    }

                #endregion

                case "DELETE /admin/replication/object":
                    #region delete_admin_replication_object

                    try
                    {
                        delObject = Common.DeserializeJson<Obj>(req.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage exception while deserializing message body for message of type DELETE /admin/replication/object, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                            true);
                    }

                    delObject.DiskPath = _ObjMgr.DiskPath(delObject, md.CurrUser);
                    if (String.IsNullOrEmpty(delObject.DiskPath))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to build disk path for delete object operation");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(),
                            true);
                    }

                    if (Common.FileExists(delObject.DiskPath))
                    {
                        if (Common.DeleteFile(delObject.DiskPath))
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "PostMessage successfully deleted " + delObject.DiskPath);
                            return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                        }

                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to delete " + delObject.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to delete object.", null).ToJson(),
                            true);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to find " + delObject.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                    }

                #endregion

                case "POST /admin/replication/container":
                    #region post_admin_replication_container
                        
                    try
                    {
                        createContainer = Common.DeserializeJson<Obj>(req.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage exception while deserializing message body for message of type DELETE /admin/replication/container, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                            true);
                    }

                    createContainer.DiskPath = _ObjMgr.DiskPath(createContainer, md.CurrUser);
                    if (String.IsNullOrEmpty(createContainer.DiskPath))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to build disk path for create container operation");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(),
                            true);
                    }

                    if (Common.DirectoryExists(createContainer.DiskPath))
                    {
                        if (Common.CreateDirectory(createContainer.DiskPath))
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "PostMessage successfully created " + createContainer.DiskPath);
                            return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                        }

                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to create " + createContainer.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to create disk path.", null).ToJson(),
                            true);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage directory " + createContainer.DiskPath + " already exists");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(7, 400, "Container already exists.", null).ToJson(),
                            true);
                    }

                #endregion

                case "POST /admin/replication/object":
                    #region post_admin_replication_object

                    try
                    {
                        createObject = Common.DeserializeJson<Obj>(req.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage exception while deserializing message body for message of type POST /admin/replication/object, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                            true);
                    }

                    createObject.DiskPath = _ObjMgr.DiskPath(createObject, md.CurrUser);
                    if (String.IsNullOrEmpty(createObject.DiskPath))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to build disk path for create object operation");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(4, 500, "Unable to build disk path from request.", null).ToJson(),
                            true);
                    }

                    if (!_Replication.ServerObjectReceiveInternal(createObject))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to store object " + createObject.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(6, 500, "Replication operation failed.", null).ToJson(),
                            true);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "PostMessage successfully processed message of type POST /admin/replication/object for " + createObject.DiskPath);
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                    }

                    #endregion

                case "POST /admin/replication/rename/object":
                    #region post_admin_replication_rename_object

                    try
                    {
                        renameObject = Common.DeserializeJson<RenameRequest>(req.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage exception while deserializing message body for message of type POST /admin/replication/rename/object, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                            true);
                    }

                    if (!_Replication.ServerObjectRenameInternal(renameObject))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to process rename object replication request");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(6, 500, "Replication operation failed.", null).ToJson(),
                            true);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "PostMessage successfully processed message of type POST /admin/replication/rename/object");
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                    }

                    #endregion

                case "POST /admin/replication/rename/container":
                    #region post_admin_replication_rename_container

                    try
                    {
                        renameContainer = Common.DeserializeJson<RenameRequest>(req.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage exception while deserializing message body for message of type POST /admin/replication/rename/container, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                            true);
                    }

                    if (!_Replication.ServerContainerRenameInternal(renameContainer))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to process rename container replication request");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(6, 500, "Replication operation failed.", null).ToJson(),
                            true);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "PostMessage successfully processed message of type POST /admin/replication/rename/container");
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                    }

                    #endregion

                case "POST /admin/replication/move/object":
                    #region post_admin_replication_move_object

                    try
                    {
                        moveObject = Common.DeserializeJson<MoveRequest>(req.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage exception while deserializing message body for message of type POST /admin/replication/move/object, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                            true);
                    }

                    if (!_Replication.ServerObjectMoveInternal(moveObject))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to process move object replication request");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(6, 500, "Replication operation failed.", null).ToJson(),
                            true);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "PostMessage successfully processed message of type POST /admin/replication/move/object");
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                    }

                    #endregion

                case "POST /admin/replication/move/container":
                    #region post_admin_replication_move_container

                    try
                    {
                        moveContainer = Common.DeserializeJson<MoveRequest>(req.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage exception while deserializing message body for message of type POST /admin/replication/move/container, returning 400");
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Unable to deserialize request body.", null).ToJson(),
                            true);
                    }

                    if (!_Replication.ServerContainerMoveInternal(moveContainer))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "PostMessage unable to process move container replication request");
                        return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                            new ErrorResponse(6, 500, "Replication operation failed.", null).ToJson(),
                            true);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "PostMessage successfully processed message of type POST /admin/replication/move/container");
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
                    }

                    #endregion

                default:
                    _Logging.Log(LoggingModule.Severity.Debug, "PostMessage received message from " + req.From.Name + " subject " + req.Subject);
                    _Logging.Log(LoggingModule.Severity.Debug, req.Data.ToString());
                    return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", null, true);
            }

            #endregion
        }
    }
}