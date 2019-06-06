using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;
using RestWrapper;

using Kvpbase.Core;

namespace Kvpbase.Classes.Managers
{
    public class EncryptionManager
    {
        #region Public-Members

        public Settings _Settings { get; set; }
        public LoggingModule _Logging { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public EncryptionManager(Settings settings, LoggingModule logging)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (settings.Encryption == null) throw new ArgumentException("Settings.Encryption is null or empty.");
            if (String.Compare(settings.Encryption.Mode, "local") != 0
                && String.Compare(settings.Encryption.Mode, "server") != 0)
            {
                throw new ArgumentException("Settings.Encryption.Mode should either be local or server.");
            }

            _Settings = settings;
            _Logging = logging;
        }

        #endregion

        #region Public-Methods

        public byte[] LocalEncrypt(byte[] clear)
        {
            if (clear == null || clear.Length < 1) throw new ArgumentNullException(nameof(clear));

            // Taken from http://www.obviex.com/samples/Encryption.aspx 
            byte[] iv = Encoding.ASCII.GetBytes(_Settings.Encryption.Iv);
            byte[] salt = Encoding.ASCII.GetBytes(_Settings.Encryption.Salt);
            PasswordDeriveBytes password = new PasswordDeriveBytes(_Settings.Encryption.Passphrase, salt, "SHA1", 2);

            byte[] key = password.GetBytes(256 / 8);
            RijndaelManaged symmetricKey = new RijndaelManaged();
            symmetricKey.Mode = CipherMode.CBC;
            ICryptoTransform encryptor = symmetricKey.CreateEncryptor(key, iv);
            MemoryStream ms = new MemoryStream();
            CryptoStream cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write);
            cs.Write(clear, 0, clear.Length);
            cs.FlushFinalBlock();
            byte[] cipherBytes = ms.ToArray();
            ms.Close();
            cs.Close();

            return cipherBytes;
        }

        public string LocalEncrypt(string clear)
        {
            if (String.IsNullOrEmpty(clear)) throw new ArgumentNullException(nameof(clear));

            // Taken from http://www.obviex.com/samples/Encryption.aspx
            byte[] iv = Encoding.ASCII.GetBytes(_Settings.Encryption.Iv);
            byte[] salt = Encoding.ASCII.GetBytes(_Settings.Encryption.Salt);
            byte[] clearBytes = Encoding.UTF8.GetBytes(clear);
            PasswordDeriveBytes password = new PasswordDeriveBytes(_Settings.Encryption.Passphrase, salt, "SHA1", 2);

            byte[] keyBytes = password.GetBytes(256 / 8);
            RijndaelManaged symmKey = new RijndaelManaged();
            symmKey.Mode = CipherMode.CBC;
            ICryptoTransform encryptor = symmKey.CreateEncryptor(keyBytes, iv);
            MemoryStream ms = new MemoryStream();
            CryptoStream cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write);

            cs.Write(clearBytes, 0, clearBytes.Length);
            cs.FlushFinalBlock();
            byte[] cipherBytes = ms.ToArray();
            ms.Close();
            cs.Close();
            string cipher = Convert.ToBase64String(cipherBytes);
             
            return cipher;
        }

        public byte[] LocalDecrypt(byte[] cipher)
        {
            if (cipher == null || cipher.Length < 1) throw new ArgumentNullException(nameof(cipher));

            // Taken from http://www.obviex.com/samples/Encryption.aspx 
            byte[] iv = Encoding.ASCII.GetBytes(_Settings.Encryption.Iv);
            byte[] salt = Encoding.ASCII.GetBytes(_Settings.Encryption.Salt);
            PasswordDeriveBytes password = new PasswordDeriveBytes(_Settings.Encryption.Passphrase, salt, "SHA1", 2);

            byte[] keyBytes = password.GetBytes(256 / 8);
            RijndaelManaged symmetricKey = new RijndaelManaged();
            symmetricKey.Mode = CipherMode.CBC;
            ICryptoTransform decryptor = symmetricKey.CreateDecryptor(keyBytes, iv);
            MemoryStream ms = new MemoryStream(cipher);
            CryptoStream cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read);
            byte[] clear = new byte[cipher.Length];
            int decryptedCount = cs.Read(clear, 0, clear.Length);
            ms.Close();
            cs.Close();

            return clear;
        }

        public string LocalDecrypt(string cipher)
        {
            if (String.IsNullOrEmpty(cipher)) throw new ArgumentNullException(nameof(cipher));

            // Taken from http://www.obviex.com/samples/Encryption.aspx
            byte[] iv = Encoding.ASCII.GetBytes(_Settings.Encryption.Iv);
            byte[] salt = Encoding.ASCII.GetBytes(_Settings.Encryption.Salt);
            byte[] cipherBytes = Convert.FromBase64String(cipher);
            PasswordDeriveBytes password = new PasswordDeriveBytes(_Settings.Encryption.Passphrase, salt, "SHA1", 2);

            byte[] keyBytes = password.GetBytes(256 / 8);
            RijndaelManaged symmetricKey = new RijndaelManaged();
            symmetricKey.Mode = CipherMode.CBC;
            ICryptoTransform decryptor = symmetricKey.CreateDecryptor(keyBytes, iv);
            MemoryStream ms = new MemoryStream(cipherBytes);
            CryptoStream cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read);
            byte[] clearBytes = new byte[cipherBytes.Length];
            int decryptedCount = cs.Read(clearBytes, 0, clearBytes.Length);
            ms.Close();
            cs.Close();
            string clear = Encoding.UTF8.GetString(clearBytes, 0, decryptedCount);

            return clear;
        }

        public bool ServerDecrypt(byte[] cipher, string ksn, out byte[] clear)
        {
            clear = null;
            if (cipher == null || cipher.Length < 1) throw new ArgumentNullException(nameof(cipher));
            if (String.IsNullOrEmpty(ksn)) throw new ArgumentNullException(nameof(ksn));
             
            string url = "";
            DateTime startTime = DateTime.Now;
            Dictionary<string, string> headers = new Dictionary<string, string>();
            EncryptedMessage msg = new EncryptedMessage();

            if (Common.IsTrue(_Settings.Encryption.Ssl)) url = "https://";
            else url = "http://";
            url += _Settings.Encryption.Server + ":" + _Settings.Encryption.Port + "/decrypt";

            headers = Common.AddToDictionary(_Settings.Encryption.ApiKeyHeader, _Settings.Encryption.ApiKeyValue, null);

            msg.Cipher = cipher;
            msg.Ksn = ksn;

            RestRequest req = new RestRequest(
                url,
                HttpMethod.POST,
                headers,
                "application/json",
                true);

            RestResponse resp = req.Send(Encoding.UTF8.GetBytes(Common.SerializeJson(msg, true)));
            
            if (resp == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerDecrypt null REST response returned");
                return false;
            }

            if (resp.StatusCode < 200 || resp.StatusCode > 299)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerDecrypt non success response returned: " + resp.StatusCode);
                return false;
            }

            clear = resp.Data;
             
            return true;
        }

        public bool ServerEncrypt(byte[] clear, out byte[] cipher, out string ksn)
        {
            cipher = null;
            ksn = "";
            if (clear == null || clear.Length < 1) throw new ArgumentNullException(nameof(clear));
              
            string url = "";
            DateTime startTime = DateTime.Now;
            Dictionary<string, string> headers = new Dictionary<string, string>();
            EncryptedMessage ret = new EncryptedMessage();

            if (Common.IsTrue(_Settings.Encryption.Ssl)) url = "https://";
            else url = "http://";
            url += _Settings.Encryption.Server + ":" + _Settings.Encryption.Port + "/encrypt";

            headers = Common.AddToDictionary(_Settings.Encryption.ApiKeyHeader, _Settings.Encryption.ApiKeyValue, null);

            RestRequest req = new RestRequest(
                url,
                HttpMethod.POST,
                headers,
                "application/octet-stream",
                true);

            RestResponse resp = req.Send(clear);

            if (resp == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerEncrypt null REST response returned");
                return false;
            }

            if (resp.StatusCode < 200 || resp.StatusCode > 299)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerEncrypt non success response returned: " + resp.StatusCode);
                return false;
            }

            try
            {
                ret = Common.DeserializeJson<EncryptedMessage>(resp.Data);
            }
            catch (Exception)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerEncrypt unable to deserialize rest response body to encrypted message");
                return false;
            }

            if (ret == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerEncrypt null response object after deserialization");
                return false;
            }

            cipher = ret.Cipher;
            ksn = ret.Ksn;
             
            return true;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
