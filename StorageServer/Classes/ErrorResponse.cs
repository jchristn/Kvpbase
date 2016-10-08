using System;

namespace Kvpbase
{
    public class ErrorResponse
    {
        #region Public-Members

        public bool Success;
        public int Id;
        public int HttpStatus;
        public string HttpText;
        public string Text;
        public object Data;

        #endregion

        #region Constructors-and-Factories

        public ErrorResponse(
            int id,
            int status,
            string textAppend,
            object data)
        {
            Id = id;
            Data = data;
            HttpStatus = status;

            // set http_status
            switch (HttpStatus)
            {
                case 200:
                    HttpText = "OK";
                    break;

                case 201:
                    HttpText = "Created";
                    break;

                case 301:
                    HttpText = "Moved Permanently";
                    break;

                case 302:
                    HttpText = "Moved Temporarily";
                    break;

                case 304:
                    HttpText = "Not Modified";
                    break;

                case 400:
                    HttpText = "Bad Request";
                    break;

                case 401:
                    HttpText = "Unauthorized";
                    break;

                case 403:
                    HttpText = "Forbidden";
                    break;

                case 404:
                    HttpText = "Not Found";
                    break;

                case 405:
                    HttpText = "Method Not Allowed";
                    break;

                case 409:
                    HttpText = "Conflict";
                    break;

                case 423:
                    HttpText = "Locked";
                    break;

                case 429:
                    HttpText = "Too Many Requests";
                    break;

                case 500:
                    HttpText = "Internal Server Error";
                    break;

                case 501:
                    HttpText = "Not Implemented";
                    break;

                case 503:
                    HttpText = "Service Unavailable";
                    break;

                default:
                    HttpText = "Unknown";
                    break;
            }

            // set text
            switch (Id)
            {
                case 1:
                    Text = "An outer exception occurred.";
                    break;

                case 2:
                    Text = "Your request was invalid.";
                    break;

                case 3:
                    Text = "You are not authorized to perform this request.";
                    break;

                case 4:
                    Text = "An error on our end was encountered.";
                    break;

                case 5:
                    Text = "The requested object was not found.";
                    break;

                case 6:
                    Text = "Unable to send message.";
                    break;

                case 7:
                    Text = "Object already exists.";
                    break;

                case 8:
                    Text = "Action handler failure.";
                    break;

                default:
                    Text = "Unknown failure code.";
                    break;
            }

            // set text_append
            if (!String.IsNullOrEmpty(textAppend))
            {
                Text += "  " + textAppend;
            }
        }

        #endregion

        #region Public-Methods

        public string ToJson()
        {
            return Common.SerializeJson(this);
        }

        #endregion
    }
}
