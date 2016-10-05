using System;
using System.Collections.Generic;
using System.IO;
using SyslogLogging;

namespace Kvpbase
{
    public class DirInfo
    {
        #region Public-Members

        public string Url { get; set; }
        public string UserGuid { get; set; }
        public string ContainerName { get; set; }
        public long Size { get; set; }
        public int NumObjects { get; set; }
        public DateTime? Created { get; set; }
        public DateTime? LastUpdate { get; set; }
        public DateTime? LastAccess { get; set; }
        public List<string> ContainerPath { get; set; }
        public List<string> ChildContainers { get; set; }
        public List<ObjInfo> ObjectMetadata { get; set; }

        #endregion

        #region Private-Members

        private Settings CurrentSettings;
        private UserManager Users;
        private Events Logging;

        #endregion

        #region Constructors-and-Factories

        public DirInfo()
        {

        }

        public DirInfo(Settings settings, UserManager users, Events logging)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (users == null) throw new ArgumentNullException(nameof(users));
            if (logging == null) throw new ArgumentNullException(nameof(logging));
                
            CurrentSettings = settings;
            Users = users;
            Logging = logging;
        }

        public DirInfo FromDirectory(string directory, string userGuid, int maxResults, List<SearchFilter> filters, bool metadataOnly)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(directory)) return null;
            if (!Common.DirectoryExists(directory)) return null;
            if (maxResults < 0) return null;

            #endregion

            #region Variables

            DirInfo ret = new DirInfo();
            ret.ChildContainers = new List<string>();
            ret.ContainerPath = new List<string>();

            DirectoryInfo di = new DirectoryInfo(directory);
            bool filterMatch = false;

            ret.ContainerName = di.Name;
            ret.ObjectMetadata = new List<ObjInfo>();
            ret.Size = 0;

            #endregion

            #region Timestamps

            ret.Created = di.CreationTimeUtc;
            ret.LastAccess = di.LastAccessTimeUtc;
            ret.LastUpdate = di.LastWriteTimeUtc;

            #endregion

            #region Retrieve-Containers

            string[] containerList = Directory.GetDirectories(directory);
            ret.ContainerPath = GetContainerList(directory, userGuid);
            if (containerList != null)
            {
                if (containerList.Length > 0)
                {
                    foreach (string currContainer in containerList)
                    {
                        string container_name = Common.StringRemove(currContainer, directory);
                        container_name = container_name.Replace(Common.GetPathSeparator(CurrentSettings.Environment), "");
                        ret.ChildContainers.Add(container_name);
                    }
                }
            }

            #endregion

            #region Retrieve-Files

            string[] fileList = Directory.GetFiles(directory, "*.*");
            if (fileList == null || fileList.Length == 0) return ret;

            #endregion

            #region Process-Each-File

            foreach (string curFile in fileList)
            {
                #region Retrieve-Object-Metadata

                // syslog_debug("get_dir_info gathering file information for " + curr_file + " in " + directory);
                ObjInfo currObjInfo = ObjInfo.FromFile(curFile);

                #endregion

                #region Process-Search-Filters

                if (filters != null)
                {
                    if (filters.Count > 0)
                    {
                        #region Reset-Variables

                        filterMatch = false;

                        #endregion

                        #region Process-Each-Filter

                        foreach (SearchFilter currFilter in filters)
                        {
                            if (String.IsNullOrEmpty(currFilter.Field)) continue;

                            switch (currFilter.Field)
                            {
                                case "Name":
                                    #region Name

                                    if (String.IsNullOrEmpty(currFilter.Condition)) continue;
                                    if (String.IsNullOrEmpty(currFilter.Value)) continue;

                                    switch (currFilter.Condition)
                                    {
                                        case "Equal":
                                            if (String.Compare(currFilter.Value.ToLower().Trim(), currObjInfo.Key.ToLower().Trim()) == 0) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        case "NotEqual":
                                            if (String.Compare(currFilter.Value.ToLower().Trim(), currObjInfo.Key.ToLower().Trim()) != 0) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        case "StartsWith":
                                            if (currObjInfo.Key.ToLower().Trim().StartsWith(currFilter.Value.ToLower().Trim())) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        case "EndsWith":
                                            if (currObjInfo.Key.ToLower().Trim().EndsWith(currFilter.Value.ToLower().Trim())) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        case "Contains":
                                            if (currObjInfo.Key.ToLower().Trim().Contains(currFilter.Value.ToLower().Trim())) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        default:
                                            break;
                                    }

                                    break;

                                #endregion

                                case "Size":
                                    #region Size

                                    if (String.IsNullOrEmpty(currFilter.Condition)) continue;
                                    if (String.IsNullOrEmpty(currFilter.Value)) continue;
                                    int filterSize = 0;

                                    if (!Int32.TryParse(currFilter.Value, out filterSize))
                                    {
                                        continue;
                                    }

                                    switch (currFilter.Condition)
                                    {
                                        case "GreaterThan":
                                            if (currObjInfo.Size > filterSize) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        case "LessThan":
                                            if (currObjInfo.Size < filterSize) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        default:
                                            break;
                                    }

                                    break;

                                #endregion

                                case "Created":
                                    #region Created

                                    if (String.IsNullOrEmpty(currFilter.Condition)) continue;
                                    if (String.IsNullOrEmpty(currFilter.Value)) continue;
                                    DateTime createdTs;

                                    try
                                    {
                                        createdTs = Convert.ToDateTime(currFilter.Value);
                                    }
                                    catch (Exception)
                                    {
                                        continue;
                                    }

                                    switch (currFilter.Condition)
                                    {
                                        case "GreaterThan":
                                            if (DateTime.Compare(Convert.ToDateTime(currObjInfo.Created), createdTs) > 0) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        case "LessThan":
                                            if (DateTime.Compare(Convert.ToDateTime(currObjInfo.Created), createdTs) < 0) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        default:
                                            break;
                                    }

                                    break;

                                #endregion

                                case "LastUpdate":
                                    #region LastUpdate

                                    if (String.IsNullOrEmpty(currFilter.Condition)) continue;
                                    if (String.IsNullOrEmpty(currFilter.Value)) continue;
                                    DateTime lastUpdateTs;

                                    try
                                    {
                                        lastUpdateTs = Convert.ToDateTime(currFilter.Value);
                                    }
                                    catch (Exception)
                                    {
                                        continue;
                                    }

                                    switch (currFilter.Condition)
                                    {
                                        case "GreaterThan":
                                            if (DateTime.Compare(Convert.ToDateTime(currObjInfo.LastUpdate), lastUpdateTs) > 0) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        case "LessThan":
                                            if (DateTime.Compare(Convert.ToDateTime(currObjInfo.LastUpdate), lastUpdateTs) < 0) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        default:
                                            break;
                                    }

                                    break;

                                #endregion

                                case "LastAccess":
                                    #region LastAccess

                                    if (String.IsNullOrEmpty(currFilter.Condition)) continue;
                                    if (String.IsNullOrEmpty(currFilter.Value)) continue;
                                    DateTime lastAccessTs;

                                    try
                                    {
                                        lastAccessTs = Convert.ToDateTime(currFilter.Value);
                                    }
                                    catch (Exception)
                                    {
                                        continue;
                                    }

                                    switch (currFilter.Condition)
                                    {
                                        case "GreaterThan":
                                            if (DateTime.Compare(Convert.ToDateTime(currObjInfo.LastAccess), lastAccessTs) > 0) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        case "LessThan":
                                            if (DateTime.Compare(Convert.ToDateTime(currObjInfo.LastAccess), lastAccessTs) < 0) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        default:
                                            break;
                                    }

                                    break;

                                #endregion

                                case "Contents":
                                    #region contents

                                    if (String.IsNullOrEmpty(currFilter.Condition)) continue;
                                    if (String.IsNullOrEmpty(currFilter.Value)) continue;

                                    string currObjContents = "";

                                    switch (currFilter.Condition)
                                    {
                                        case "Contains":
                                            currObjContents = Common.ReadTextFile(directory + Common.GetPathSeparator(CurrentSettings.Environment) + currObjInfo.Key);
                                            if (String.IsNullOrEmpty(currObjContents)) continue;

                                            if (currObjContents.ToLower().Contains(currFilter.Value.ToLower())) filterMatch = true;
                                            else filterMatch = false;
                                            break;

                                        case "ContainsNot":
                                            currObjContents = Common.ReadTextFile(directory + Common.GetPathSeparator(CurrentSettings.Environment) + currObjInfo.Key);
                                            if (String.IsNullOrEmpty(currObjContents)) continue;

                                            if (currObjContents.ToLower().Contains(currFilter.Value.ToLower())) filterMatch = false;
                                            else filterMatch = true;
                                            break;

                                        default:
                                            break;
                                    }

                                    break;

                                #endregion

                                default:
                                    break;
                            }
                        }

                        #endregion

                        #region Check-Filter-Match

                        if (!filterMatch) continue;

                        #endregion
                    }
                }

                #endregion

                #region Check-Max-Results

                if (maxResults != 0)
                {
                    if (ret.NumObjects == maxResults)
                    {
                        break;
                    }
                }

                #endregion

                #region Add-to-List-and-Update-Stats

                ret.NumObjects++;
                ret.Size += currObjInfo.Size;

                if (!metadataOnly) ret.ObjectMetadata.Add(currObjInfo);

                #endregion
            }

            #endregion

            return ret;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion

        #region Public-Static-Methods

        #endregion

        #region Private-Static-Methods

        public List<string> GetContainerList(string path, string userGuid)
        {
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(path)) return null;

            #endregion

            #region Variables

            string homeDirectory = "";
            List<string> ret = new List<string>();
            string reduced = String.Copy(path);
            string tempString = "";

            #endregion

            #region Retrieve-User-Home-Directory

            homeDirectory = Users.GetHomeDirectory(userGuid, CurrentSettings, Logging);
            if (String.IsNullOrEmpty(homeDirectory)) return null;

            #endregion

            #region Process
            
            reduced = reduced.Replace(homeDirectory + Common.GetPathSeparator(CurrentSettings.Environment), "");
            reduced = reduced.Replace(homeDirectory, "");
            
            foreach (char c in reduced)
            {
                if (String.Compare(c.ToString(), Common.GetPathSeparator(CurrentSettings.Environment)) == 0)
                {
                    if (!String.IsNullOrEmpty(tempString))
                    {
                        if (String.Compare(tempString, Common.GetPathSeparator(CurrentSettings.Environment)) == 0)
                        {
                            tempString = "";
                            continue;
                        }
                        
                        ret.Add(tempString);
                        tempString = "";
                        continue;
                    }
                }

                tempString += c;
            }

            if (!String.IsNullOrEmpty(tempString))
            {
                ret.Add(tempString);
                tempString = "";
            }

            return ret;

            #endregion
        }

        #endregion
    }
}
