using System;
using System.Net;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        static HttpResponse AdminApiHandler(RequestMetadata md)
        {
            #region Enumerate

            _Logging.Log(LoggingModule.Severity.Debug, 
                "AdminApiHandler admin API requested by " + 
                md.CurrHttpReq.SourceIp + ":" + md.CurrHttpReq.SourcePort + " " + 
                md.CurrHttpReq.Method + " " + md.CurrHttpReq.RawUrlWithoutQuery);

            #endregion

            #region Variables

            string reqMetadataVal = "";
            bool reqMetadata = false;

            #endregion

            #region Check-for-Metadata-Request

            reqMetadataVal = md.CurrHttpReq.RetrieveHeaderValue("request_metadata");
            reqMetadata = Common.IsTrue(reqMetadataVal);
                
            if (reqMetadata)
            {
                return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(md), true);
            }

            #endregion

            #region Process-Request

            switch (md.CurrHttpReq.Method.ToLower())
            {
                case "get":
                    #region get

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/cleanup", false))
                    {
                        return GetCleanup(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/connections", false))
                    {
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(_ConnMgr.GetActiveConnections()), true);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/disks", false))
                    {
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(DiskInfo.GetAllDisks()), true);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/heartbeat", false))
                    {
                        return GetHeartbeat(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/messages/count", false))
                    {
                        return GetMessagesCount(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/neighbors", false))
                    {
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(_Topology.Replicas), true);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/replication/count", false))
                    {
                        return GetReplicationCount(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/tasks/count", false))
                    {
                        return GetTasksCount(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/topology", false))
                    {
                        return new HttpResponse(md.CurrHttpReq, true, 200, null, "application/json", Common.SerializeJson(_Topology), true);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/login", false))
                    {
                        return GetLogin(md);
                    }

                    break;

                #endregion

                case "put":
                    #region put

                    break;

                #endregion

                case "post":
                    #region post

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/find", false))
                    {
                        return PostFind(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/kill", false))
                    {
                        return PostKill(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/message", false))
                    {
                        return PostMessage(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/owner", false))
                    {
                        return PostOwner(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/replication/container", false))
                    {
                        return _Replication.ServerContainerReceive(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/replication/object", false))
                    {
                        return _Replication.ServerObjectReceive(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/replication/move/container", false))
                    {
                        return _Replication.ServerContainerMove(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/replication/move/object", false))
                    {
                        return _Replication.ServerObjectMove(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/replication/rename/container", false))
                    {
                        return _Replication.ServerContainerRename(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/replication/rename/object", false))
                    {
                        return _Replication.ServerObjectRename(md);
                    }

                    break;

                #endregion

                case "delete":
                    #region delete

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/replication/container", false))
                    {
                        return DeleteReplicationContainer(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/replication/object", false))
                    {
                        return DeleteReplicationObject(md);
                    }

                    if (WatsonCommon.UrlEqual(md.CurrHttpReq.RawUrlWithoutQuery, "/admin/user_guid", false))
                    {
                        return DeleteUserGuid(md);
                    }

                    break;

                #endregion

                case "head":
                    #region head

                    break;

                #endregion

                default:
                    _Logging.Log(LoggingModule.Severity.Warn, "AdminApiHandler unknown http method: " + md.CurrHttpReq.Method);
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unsupported HTTP method.", null).ToJson(),
                        true);
            }

            _Logging.Log(LoggingModule.Severity.Warn, "AdminApiHandler unknown endpoint URL: " + md.CurrHttpReq.RawUrlWithoutQuery);
            return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json", 
                new ErrorResponse(2, 400, "Unknown endpoint.", null), true);

            #endregion
        }
    }
}