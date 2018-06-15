using System;

namespace Kvpbase
{
    /// <summary>
    /// Object containing error-related information, used in HTTP API responses.
    /// </summary>
    public class ErrorResponse
    {
        #region Public-Members

        /// <summary>
        /// Indicates success or failure of the operation.
        /// </summary>
        public bool Success;

        /// <summary>
        /// The ID of the error.
        /// </summary>
        public int Id;

        /// <summary>
        /// The HTTP status of the error.
        /// </summary>
        public int HttpStatus;

        /// <summary>
        /// The generic HTTP text for the error.
        /// </summary>
        public string HttpText;

        /// <summary>
        /// The human readable text for the error.
        /// </summary>
        public string Text;

        /// <summary>
        /// Any additional data associated with the error.
        /// </summary>
        public object Data;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        /// <param name="id">The ID of the error.</param>
        /// <param name="status">The HTTP status of the error.</param>
        /// <param name="textAppend">Text to append to the human-readable error text.</param>
        /// <param name="data">Data associated with the error.</param>
        public ErrorResponse(
            int id,
            int status,
            string textAppend,
            object data)
        {
            Id = id;
            Data = data;
            HttpStatus = status;
             
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

                case 413:
                    HttpText = "Payload Too Large";
                    break;

                case 416:
                    HttpText = "Requested Range Not Satisfiable";
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
                    Text = "Resource in use.";
                    break;

                case 9:
                    Text = "Deserialization error.";
                    break;

                case 10:
                    Text = "Replication failure.";
                    break;

                case 11:
                    Text = "Request too large; use smaller ranges.";
                    break;

                default:
                    Text = "Unknown failure code.";
                    break;
            }
             
            if (!String.IsNullOrEmpty(textAppend))
            {
                Text += "  " + textAppend;
            }
        }

        #endregion

        #region Public-Methods
         
        #endregion

        #region Private-Methods

        #endregion
    }
}
