using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using SyslogLogging;
using WatsonWebserver;

namespace Kvpbase
{
    public partial class StorageServer
    {
        public static HttpResponse GetPublicObject(RequestMetadata md)
        {
            DateTime startTime = DateTime.Now;
            bool locked = false;

            try
            {
                #region try
                
                #region Variables

                string readFromVal = "";
                int readFrom = 0;
                string countVal = "";
                int count = 0;
                string pubfileGuid = "";
                string pubfileContents = "";
                PublicObj currPubfile = new PublicObj();
                Obj currObj = new Obj();
                ObjInfo currObjInfo = new ObjInfo();
                byte[] clear;
                Dictionary<string, string> responseHeaders = null;
                string containerLogFile = "";
                string containerPropertiesFile = "";
                ContainerPropertiesFile currContainerPropertiesFile = new ContainerPropertiesFile();
                string objectLogFile = "";
                string objectPropertiesFile = "";
                ObjectPropertiesFile currObjectPropertiesFile = new ObjectPropertiesFile();

                #endregion

                #region Get-Values-from-Querystring

                readFromVal = md.CurrHttpReq.RetrieveHeaderValue("read_from");
                if (!String.IsNullOrEmpty(readFromVal))
                {
                    if (!Int32.TryParse(readFromVal, out readFrom))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile invalid value for read_from in querystring: " + readFromVal);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for read_from.", null).ToJson(), true);
                    }

                    if (readFrom < 0)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile invalid value for read_from (must be zero or greater): " + readFrom);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for read_from.", null).ToJson(), true);
                    }
                }

                countVal = md.CurrHttpReq.RetrieveHeaderValue("count");
                if (!String.IsNullOrEmpty(countVal))
                {
                    if (!Int32.TryParse(countVal, out count))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile invalid value for count in querystring: " + countVal);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for count.", null).ToJson(), true);
                    }

                    if (count < 1)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile invalid value for count (must be greater than zero): " + count);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for count.", null).ToJson(), true);
                    }
                }

                #endregion

                #region Read-Pubfile

                pubfileGuid = md.CurrHttpReq.RawUrlWithoutQuery.Replace("/public/", "");
                _Logging.Log(LoggingModule.Severity.Debug, "GetPublicFile URL " + md.CurrHttpReq.RawUrlWithoutQuery + " (pubfile GUID " + pubfileGuid + ")");

                if (String.IsNullOrEmpty(pubfileGuid))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile null GUID after removing base URL");
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to process URL.", null).ToJson(), true);
                }

                if (pubfileGuid.Contains("/")
                    || pubfileGuid.Contains(".")
                    || pubfileGuid.Contains("?")
                    || pubfileGuid.Contains("=")
                    || pubfileGuid.Contains("&")
                    )
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile URL contains invalid characters: " + pubfileGuid);
                    return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid URL.", null).ToJson(), true);
                }

                pubfileContents = Common.ReadTextFile(_Settings.PublicObj.Directory + pubfileGuid);
                if (String.IsNullOrEmpty(pubfileContents))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to read contents of pubfile " + _Settings.PublicObj.Directory + " (null)");
                    return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                }

                try
                {
                    currPubfile = Common.DeserializeJson<PublicObj>(pubfileContents);
                }
                catch (Exception)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to deserialize pubfile at " + _Settings.PublicObj.Directory + pubfileGuid);
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(1, 500, null, null).ToJson(), true);
                }

                if (DateTime.Compare(DateTime.Now.ToUniversalTime(), currPubfile.Expiration) > 0)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile pubfile " + _Settings.PublicObj.Directory + pubfileGuid + " expired, deleting (expired " + currPubfile.Expiration.ToString("MM/dd/yyyy HH:mm:ss"));

                    if (!Common.DeleteFile(_Settings.PublicObj.Directory + pubfileGuid))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to delete pubfile " + _Settings.PublicObj.Directory + pubfileGuid);
                    }

                    return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                }

                #endregion

                #region Rewrite-Metadata-Object

                currObj = _ObjMgr.BuildObjFromDisk(currPubfile.DiskPath);
                if (currObj == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to overwrite metadata object using disk path " + currPubfile.DiskPath);
                    return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                        new ErrorResponse(1, 500, null, null).ToJson(), true);
                }

                md.CurrObj = currObj;

                #endregion

                #region Retrieve-Specific-Object
                
                #region Add-Lock

                locked = _UrlLockMgr.AddReadResource(md.CurrObj.DiskPath);
                if (!locked)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile " + md.CurrObj.DiskPath + " is unable to be locked");
                    return new HttpResponse(md.CurrHttpReq, false, 401, null, "application/json",
                        new ErrorResponse(9, 423, "Resource in use.", null).ToJson(), true);
                }

                #endregion

                #region Check-Container-Permissions-and-Logging

                currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrObj, out containerLogFile, out containerPropertiesFile);
                if (currContainerPropertiesFile != null)
                {
                    if (currContainerPropertiesFile.Logging != null)
                    {
                        if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.ReadObject))
                            {
                                #region Process-Logging

                                _Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "GetPublicFile", null));

                                #endregion
                            }
                        }
                    }
                }

                #endregion

                #region Check-Object-Permissions-and-Logging

                currObjectPropertiesFile = ObjectPropertiesFile.FromObject(md.CurrObj, out objectLogFile, out objectPropertiesFile);
                if (currObjectPropertiesFile != null)
                {
                    if (currObjectPropertiesFile.Logging != null)
                    {
                        if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                        {
                            if (Common.IsTrue(currObjectPropertiesFile.Logging.ReadObject))
                            {
                                #region Process-Logging

                                _Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "GetPublicFile", null));

                                #endregion
                            }
                        }
                    }
                }
              
                #endregion

                #region Retrieve-Object-Metadata

                currObjInfo = ObjInfo.FromFile(md.CurrObj.DiskPath);
                if (currObjInfo == null)
                {
                    return new HttpResponse(md.CurrHttpReq, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                }

                #endregion

                #region Decrypt

                if (Common.IsTrue(md.CurrObj.IsEncrypted))
                {
                    if (String.IsNullOrEmpty(md.CurrObj.EncryptionKsn))
                    {
                        md.CurrObj.Value = _EncryptionMgr.LocalDecrypt(md.CurrObj.Value);
                    }
                    else
                    {
                        if (!_EncryptionMgr.ServerDecrypt(md.CurrObj.Value, md.CurrObj.EncryptionKsn, out clear))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to decrypt object using server-based decryption");
                            return new HttpResponse(md.CurrHttpReq, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to decrypt object using crypto server.", null).ToJson(), true);
                        }

                        md.CurrObj.Value = clear;
                    }
                }

                #endregion

                #region Decompress

                if (Common.IsTrue(md.CurrObj.IsCompressed))
                {
                    if (Common.IsTrue(_Settings.Debug.DebugCompression)) _Logging.Log(LoggingModule.Severity.Debug, "GetPublicFile before decompression: " + Common.BytesToBase64(md.CurrObj.Value));
                    md.CurrObj.Value = Common.GzipDecompress(md.CurrObj.Value);
                    if (Common.IsTrue(_Settings.Debug.DebugCompression)) _Logging.Log(LoggingModule.Severity.Debug, "GetPublicFile after decompression: " + Common.BytesToBase64(md.CurrObj.Value));
                }

                #endregion

                #region Set-Content-Type
                
                if (!String.IsNullOrEmpty(md.CurrObj.ContentType))
                {
                    responseHeaders = new Dictionary<string, string>();
                    responseHeaders.Add("content-type", md.CurrObj.ContentType);
                }
                else
                {
                    string ContentType = MimeTypes.GetFromExtension(Common.GetFileExtension(md.CurrObj.DiskPath));
                    responseHeaders = new Dictionary<string, string>();
                    responseHeaders.Add("content-type", ContentType );
                }
               
                #endregion

                #region Validate-Range-Read

                if (count > 0)
                {
                    if (readFrom + count > md.CurrObj.Value.Length)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile range exceeds object length (" + md.CurrObj.Value.Length + "): read_from " + readFrom + " count " + count);
                        return new HttpResponse(md.CurrHttpReq, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Range exceeds object length.", null).ToJson(), true);
                    }
                }

                #endregion
                
                #region Respond
                
                if (count > 0)
                {
                    byte[] ret = new byte[count];
                    Buffer.BlockCopy(md.CurrObj.Value, readFrom, ret, 0, count);
                    return new HttpResponse(md.CurrHttpReq, true, 200, responseHeaders, null, ret, true);
                }
                else
                {
                    return new HttpResponse(md.CurrHttpReq, true, 200, responseHeaders, null, md.CurrObj.Value, true);
                }                    

                #endregion

                #endregion
                
                #endregion
            }
            finally
            {
                #region finally

                #region unlock

                if (locked)
                {
                    if (!_UrlLockMgr.RemoveReadResource(md.CurrObj.DiskPath))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to unlock " + md.CurrObj.DiskPath);
                    }
                }

                #endregion
                
                #endregion
            }
        }
    }
}