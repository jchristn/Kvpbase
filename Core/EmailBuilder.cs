using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web;
using System.Web.Script.Serialization;
using System.Xml;
using System.Xml.Linq;
using System.Xml.XPath;
using Newtonsoft.Json;

namespace Kvpbase
{
    /// <summary>
    /// Static methods used for building email bodies.
    /// </summary>
    public static class EmailBuilder
    {
        #region Public-Methods

        public static string BigHeader(string text)
        {
            return "<p><h2>" + text + "</h2></p>";
        }

        public static string Bottom( 
            string homepageUrl,
            bool includeEmailSentTo,
            string emailAddress)
        {
            string body = "";

            // close inner table
            body += "         </td>";
            body += "         <td width='30'>";
            body += "         </td>";
            body += "         </tr>";
            body += "        </tbody>";
            body += "       </table>";
             
            // add unsubscribe and email sent to
            if (includeEmailSentTo)
            {
                body += "<br /><br />";
                body += "<p>";
                body += " <font color='A4A4A4'>";
                body += "  This email was sent to <a href='mailto:" + emailAddress + "'>" + emailAddress + "</a>.&nbsp;&nbsp;";
                body += " </font>";
                body += "</p>";
            }

            // close outer table
            body += "      </td>";
            body += "     </tr>";
            body += "    </tbody>";
            body += "   </table>";

            // close outer div
            body += "  </div>";
            body += " </body_bytes>";
            body += "</html>";

            return body;
        }

        public static string ClosingSupport(string emailAddress)
        {
            return "<p>If you would like to contact with us, don't hesitate to email <a href='mailto:" + emailAddress + "'>" + emailAddress + "</a>.</p>";
        }

        public static string ClosingThanks(string homepageUrl)
        {
            return "<p>Thanks!</p><p><a href='" + homepageUrl + "'>" + homepageUrl + "</a></p>";
        }

        public static string Link(string url, string text, string link)
        {
            if (!String.IsNullOrEmpty(link))
            {
                return
                    "<br />" +
                    "<p>" +
                    "  <a href='" + link + "'>" +
                    "    <img src='" + url + "' alt='" + text + "'></img>" +
                    "  </a>" +
                    "</p>";
            }
            else
            {
                return
                    "<br />" +
                    "<p>" +
                    "  <img src='" + url + "' alt='" + text + "'></img>" +
                    "</p>";
            }
        }

        public static string Paragraph(string text)
        {
            return "<p>" + text + "</p>";
        }

        public static string SmallHeader(string text)
        {
            return "<p><h3>" + text + "</h3></p>";
        }

        public static string Top(string title, bool includeLogo, string logoUrl, string homepageUrl)
        {
            string body = "";

            body += "<html xmlns='http://www.w3.org/1999/xhtml'>";
            body += " <head>";
            body += "  <meta http-equiv='Content-Type' content='text/html; charset=utf-8' />";
            body += "  <meta charset='ISO-8859-1' />";
            body += "  <title>" + title + "</title>";
            body += "  <style type='text/css'>";
            body += "   #outlook a { padding:0; }";
            body += "   body_bytes { width:100% !important; } .ReadMsgBody { width:100%; } .ExternalClass{width:100%;}";
            body += "   body_bytes { -webkit-text-size-adjust:none; -ms-text-size-adjust:none;}";
            body += "   body_bytes { margin:0; padding:0; }";
            body += "   img { height:auto; line-height:100%; outline:none; text-decoration:none; border:0; }";
            body += "   a img { border:0; }";
            body += "   #backgroundTable { height:100% !important; margin:0; padding:0; width:100% !important; }";
            body += "   p { margin: 1em 0; }";
            body += "   h1, h2, h3, h4, h5, h6 { color: black !important; line-height: 100% !important; }";
            body += "   h1 a, h2 a, h3 a, h4 a, h5 a, h6 a { color: blue !important; }";
            body += "   h1 a:active, h2 a:active,  h3 a:active, h4 a:active, h5 a:active, h6 a:active { color: red !important; }";
            body += "   h1 a:visited, h2 a:visited,  h3 a:visited, h4 a:visited, h5 a:visited, h6 a:visited { color: purple !important; }";
            body += "   table td { border-collapse:collapse; }";
            body += "   .yshortcuts, .yshortcuts a, .yshortcuts a:link,.yshortcuts a:visited, .yshortcuts a:hover, .yshortcuts a span { color: black; text-decoration: none !important; border-bottom: none !important; background: none !important; }";
            body += "  </style>";
            body += " </head>";

            // body_bytes and outer table
            body += " <body_bytes>";
            body += "  <div marginheight='0' marginwidth='0' style='background:#fff; margin:0; padding:18px' bgcolor:'#ffffff'>";
            body += "   <table cellspacing='0' border='0' cellpadding='0' width='100%' align='center' style='margin:0'>";
            body += "    <tbody>";
            body += "     <tr valign='top'>";
            body += "      <td valign='top' bgcolor='#E9EFF2' style='background:#fff'>";

            if (includeLogo)
            {
                body += "       <table width='1200' cellspacing='0' cellpadding='0' border='0' align='center'>";
                body += "        <tbody>";
                body += "         <tr valign='top'>";
                body += "          <td height='80' width='1200' style='background: url('" + logoUrl + "') left no-repeat; font-family:helvetica neue, arial, helvetica, sans-serif; letter-spacing:-0.03em; text-decoration:none; text-align:left;padding:10px 30px 4px; vertical-align:middle'>";
                body += "           <a style='font-family:helvetica neue, arial, helvetica, sans-serif; text-decoration:none; text-shadow:black 0 1px 0px' href='" + homepageUrl + "' target='_blank'><img src='" + logoUrl + "'></a>";
                body += "          </td>";
                body += "         </tr>";
                body += "        </tbody>";
                body += "       </table>";
            }

            // inner table
            body += "       <br />";
            body += "       <table align='center' bgcolor='#ffffff' border='0' cellspacing='0' cellpadding='0' width='1200' style='background:#ffffff; padding-bottom:25px'>";
            body += "        <tbody>";
            body += "         <tr>"; 
            body += "          <td style='font-color: #222222; font-family:helvetica neue, arial, helvetica, sans-serif; font-color:#222222; font-size:14px; line-height:24px'>";

            return body;
        }

        #endregion
    }
}
