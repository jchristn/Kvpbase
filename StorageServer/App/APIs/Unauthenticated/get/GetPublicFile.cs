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

                readFromVal = md.CurrentHttpRequest.RetrieveHeaderValue("read_from");
                if (!String.IsNullOrEmpty(readFromVal))
                {
                    if (!Int32.TryParse(readFromVal, out readFrom))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile invalid value for read_from in querystring: " + readFromVal);
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for read_from.", null).ToJson(), true);
                    }

                    if (readFrom < 0)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile invalid value for read_from (must be zero or greater): " + readFrom);
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for read_from.", null).ToJson(), true);
                    }
                }

                countVal = md.CurrentHttpRequest.RetrieveHeaderValue("count");
                if (!String.IsNullOrEmpty(countVal))
                {
                    if (!Int32.TryParse(countVal, out count))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile invalid value for count in querystring: " + countVal);
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for count.", null).ToJson(), true);
                    }

                    if (count < 1)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile invalid value for count (must be greater than zero): " + count);
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Invalid value for count.", null).ToJson(), true);
                    }
                }

                #endregion

                #region Read-Pubfile

                pubfileGuid = md.CurrentHttpRequest.RawUrlWithoutQuery.Replace("/public/", "");
                Logging.Log(LoggingModule.Severity.Debug, "GetPublicFile URL " + md.CurrentHttpRequest.RawUrlWithoutQuery + " (pubfile GUID " + pubfileGuid + ")");

                if (String.IsNullOrEmpty(pubfileGuid))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile null GUID after removing base URL");
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Unable to process URL.", null).ToJson(), true);
                }

                if (pubfileGuid.Contains("/")
                    || pubfileGuid.Contains(".")
                    || pubfileGuid.Contains("?")
                    || pubfileGuid.Contains("=")
                    || pubfileGuid.Contains("&")
                    )
                {
                    Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile URL contains invalid characters: " + pubfileGuid);
                    return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                        new ErrorResponse(2, 400, "Invalid URL.", null).ToJson(), true);
                }

                pubfileContents = Common.ReadTextFile(CurrentSettings.PublicObj.Directory + pubfileGuid);
                if (String.IsNullOrEmpty(pubfileContents))
                {
                    Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to read contents of pubfile " + CurrentSettings.PublicObj.Directory + " (null)");
                    return new HttpResponse(md.CurrentHttpRequest, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                }

                try
                {
                    currPubfile = Common.DeserializeJson<PublicObj>(pubfileContents);
                }
                catch (Exception)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to deserialize pubfile at " + CurrentSettings.PublicObj.Directory + pubfileGuid);
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(1, 500, null, null).ToJson(), true);
                }

                if (DateTime.Compare(DateTime.Now.ToUniversalTime(), currPubfile.Expiration) > 0)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile pubfile " + CurrentSettings.PublicObj.Directory + pubfileGuid + " expired, deleting (expired " + currPubfile.Expiration.ToString("MM/dd/yyyy HH:mm:ss"));

                    if (!Common.DeleteFile(CurrentSettings.PublicObj.Directory + pubfileGuid))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to delete pubfile " + CurrentSettings.PublicObj.Directory + pubfileGuid);
                    }

                    return new HttpResponse(md.CurrentHttpRequest, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                }

                #endregion

                #region Rewrite-Metadata-Object

                currObj = Obj.BuildObjFromDisk(currPubfile.DiskPath, Users, CurrentSettings, CurrentTopology, CurrentNode, Logging);
                if (currObj == null)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to overwrite metadata object using disk path " + currPubfile.DiskPath);
                    return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                        new ErrorResponse(1, 500, null, null).ToJson(), true);
                }

                md.CurrentObj = currObj;

                #endregion

                #region Retrieve-Specific-Object
                
                #region Add-Lock

                locked = LockManager.LockUrl(md);
                if (!locked)
                {
                    Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile " + md.CurrentObj.DiskPath + " is unable to be locked");
                    return new HttpResponse(md.CurrentHttpRequest, false, 401, null, "application/json",
                        new ErrorResponse(9, 423, "Resource in use.", null).ToJson(), true);
                }

                #endregion

                #region Check-Container-Permissions-and-Logging

                currContainerPropertiesFile = ContainerPropertiesFile.FromObject(md.CurrentObj, out containerLogFile, out containerPropertiesFile);
                if (currContainerPropertiesFile != null)
                {
                    if (currContainerPropertiesFile.Logging != null)
                    {
                        if (Common.IsTrue(currContainerPropertiesFile.Logging.Enabled))
                        {
                            if (Common.IsTrue(currContainerPropertiesFile.Logging.ReadObject))
                            {
                                #region Process-Logging

                                Logger.Add(containerLogFile, LoggerManager.BuildMessage(md, "GetPublicFile", null));

                                #endregion
                            }
                        }
                    }
                }

                #endregion

                #region Check-Object-Permissions-and-Logging

                currObjectPropertiesFile = ObjectPropertiesFile.FromObject(md.CurrentObj, out objectLogFile, out objectPropertiesFile);
                if (currObjectPropertiesFile != null)
                {
                    if (currObjectPropertiesFile.Logging != null)
                    {
                        if (Common.IsTrue(currObjectPropertiesFile.Logging.Enabled))
                        {
                            if (Common.IsTrue(currObjectPropertiesFile.Logging.ReadObject))
                            {
                                #region Process-Logging

                                Logger.Add(objectLogFile, LoggerManager.BuildMessage(md, "GetPublicFile", null));

                                #endregion
                            }
                        }
                    }
                }
              
                #endregion

                #region Retrieve-Object-Metadata

                currObjInfo = ObjInfo.FromFile(md.CurrentObj.DiskPath);
                if (currObjInfo == null)
                {
                    return new HttpResponse(md.CurrentHttpRequest, false, 404, null, "application/json",
                        new ErrorResponse(5, 404, "Object does not exist.", null).ToJson(), true);
                }

                #endregion

                #region Decrypt

                if (Common.IsTrue(md.CurrentObj.IsEncrypted))
                {
                    if (String.IsNullOrEmpty(md.CurrentObj.EncryptionKsn))
                    {
                        md.CurrentObj.Value = EncryptionManager.LocalDecrypt(md.CurrentObj.Value);
                    }
                    else
                    {
                        if (!EncryptionManager.ServerDecrypt(md.CurrentObj.Value, md.CurrentObj.EncryptionKsn, out clear))
                        {
                            Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to decrypt object using server-based decryption");
                            return new HttpResponse(md.CurrentHttpRequest, false, 500, null, "application/json",
                                new ErrorResponse(4, 500, "Unable to decrypt object using crypto server.", null).ToJson(), true);
                        }

                        md.CurrentObj.Value = clear;
                    }
                }

                #endregion

                #region Decompress

                if (Common.IsTrue(md.CurrentObj.IsCompressed))
                {
                    if (Common.IsTrue(CurrentSettings.Debug.DebugCompression)) Logging.Log(LoggingModule.Severity.Debug, "GetPublicFile before decompression: " + Common.BytesToBase64(md.CurrentObj.Value));
                    md.CurrentObj.Value = Common.GzipDecompress(md.CurrentObj.Value);
                    if (Common.IsTrue(CurrentSettings.Debug.DebugCompression)) Logging.Log(LoggingModule.Severity.Debug, "GetPublicFile after decompression: " + Common.BytesToBase64(md.CurrentObj.Value));
                }

                #endregion

                #region Set-Content-Type
                
                if (!String.IsNullOrEmpty(md.CurrentObj.ContentType))
                {
                    responseHeaders = new Dictionary<string, string>();
                    responseHeaders.Add("content-type", md.CurrentObj.ContentType);
                }
                else
                {
                    string ContentType = MimeTypes.GetFromExtension(Common.GetFileExtension(md.CurrentObj.DiskPath));
                    responseHeaders = new Dictionary<string, string>();
                    responseHeaders.Add("content-type", ContentType );
                }
               
                #endregion

                #region Validate-Range-Read

                if (count > 0)
                {
                    if (readFrom + count > md.CurrentObj.Value.Length)
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile range exceeds object length (" + md.CurrentObj.Value.Length + "): read_from " + readFrom + " count " + count);
                        return new HttpResponse(md.CurrentHttpRequest, false, 400, null, "application/json",
                            new ErrorResponse(2, 400, "Range exceeds object length.", null).ToJson(), true);
                    }
                }

                #endregion
                
                #region Respond
                
                if (count > 0)
                {
                    byte[] ret = new byte[count];
                    Buffer.BlockCopy(md.CurrentObj.Value, readFrom, ret, 0, count);
                    return new HttpResponse(md.CurrentHttpRequest, true, 200, responseHeaders, null, ret, true);
                }
                else
                {
                    return new HttpResponse(md.CurrentHttpRequest, true, 200, responseHeaders, null, md.CurrentObj.Value, true);
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
                    if (!LockManager.UnlockUrl(md))
                    {
                        Logging.Log(LoggingModule.Severity.Warn, "GetPublicFile unable to unlock " + md.CurrentHttpRequest.RawUrlWithoutQuery);
                    }
                }

                #endregion
                
                #endregion
            }
        }
    }
}