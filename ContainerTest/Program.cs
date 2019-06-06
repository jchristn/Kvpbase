using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Kvpbase.Containers;
using Kvpbase.Core;

namespace Kvpbase
{
    class Program
    {  
        static Container _Container;
        static ContainerSettings _ContainerSettings;
        static bool _RunForever = true;

        static void Main(string[] args)
        { 
            _ContainerSettings = new ContainerSettings();
            _Container = new Container(_ContainerSettings);

            while (_RunForever)
            {
                string file;
                long fileLen;
                string key; 
                ContainerMetadata md; 
                string data;
                string contentType;
                int position;
                int count;
                byte[] bytes;
                ErrorCode error;
                List<string> tags;

                string userInput = Common.InputString("container [? for help] >", null, false);

                switch (userInput)
                {
                    case "?":
                        Menu();
                        break;

                    case "c":
                    case "cls":
                        Console.Clear();
                        break;

                    case "q":
                    case "quit":
                        _RunForever = false;
                        break;

                    case "write":
                        key = Common.InputString("Key:", null, false);
                        data = Common.InputString("Data:", null, true);
                        contentType = Common.InputString("Content Type:", "application/octet-stream", false);
                        tags = Common.InputStringList("Tags:", true);

                        if (_Container.WriteObject(key, contentType, Encoding.UTF8.GetBytes(data), tags, out error))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Error: " + error.ToString());
                        }
                        break;

                    case "writefile":
                        file = Common.InputString("File:", null, false);
                        fileLen = new FileInfo(file).Length;
                        key = Common.InputString("Key:", null, false);

                        using (FileStream fs = new FileStream(file, FileMode.Open))
                        {
                            if (_Container.WriteObject(key, null, fileLen, fs, null, out error))
                            {
                                Console.WriteLine("Success");
                            }
                            else
                            {
                                Console.WriteLine("Error: " + error.ToString());
                            }
                        }
                        break;

                    case "writerange":
                        key = Common.InputString("Key:", null, false);
                        position = Common.InputInteger("Position:", 0, true, true);
                        data = Common.InputString("Data:", null, true);

                        if (_Container.WriteRangeObject(key, position, Encoding.UTF8.GetBytes(data), out error))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Error: " + error.ToString());
                        }
                        break;

                    case "read":
                        key = Common.InputString("Key:", null, false);

                        if (_Container.ReadObject(key, out contentType, out bytes, out error))
                        {
                            Console.WriteLine("Success");
                            Console.WriteLine("ContentType: " + contentType);
                            Console.WriteLine(Encoding.UTF8.GetString(bytes));
                        }
                        else
                        {
                            Console.WriteLine("Error: " + error.ToString());
                        }
                        break;

                    case "readfile":
                        key = Common.InputString("Key:", null, false);
                        file = Common.InputString("File:", null, false); 
                        
                        using (FileStream fs = new FileStream(file, FileMode.OpenOrCreate))
                        {
                            Stream s = null;
                            if (_Container.ReadObject(key, out contentType, out fileLen, out s, out error))
                            {
                                int bytesRead = 0;
                                byte[] buffer = new byte[65536];
                                while ((bytesRead = s.Read(buffer, 0, buffer.Length)) > 0)
                                {
                                    fs.Write(buffer, 0, bytesRead);
                                }
                                Console.WriteLine("Success");
                            }
                            else
                            {
                                Console.WriteLine("Error: " + error.ToString());
                            }
                        }
                        break; 

                    case "readrange":
                        key = Common.InputString("Key:", null, false);
                        position = Common.InputInteger("Position:", 0, true, true);
                        count = Common.InputInteger("Count:", 0, true, true);

                        if (_Container.ReadRangeObject(key, position, count, out contentType, out bytes, out error))
                        {
                            Console.WriteLine("Success");
                            Console.WriteLine("ContentType: " + contentType);
                            Console.WriteLine(Encoding.UTF8.GetString(bytes));
                        }
                        else
                        {
                            Console.WriteLine("Error: " + error.ToString());
                        }
                        break;

                    case "remove":
                        key = Common.InputString("Key:", null, false);

                        if (_Container.RemoveObject(key, out error))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Error: " + error.ToString());
                        }
                        break;

                    case "exists":
                        key = Common.InputString("Key:", null, false);
                        Console.WriteLine("Exists: " + _Container.Exists(key));
                        break;

                    case "list":
                        md = _Container.Enumerate(null, null, null, null);
                        if (md != null)
                        {
                            Console.WriteLine(Common.SerializeJson(md, true)); 
                        }
                        else
                        {
                            Console.WriteLine("None");
                        }
                        break;
                }
            }
        }

        static void Menu()
        {
            Console.WriteLine("Available commands:");
            Console.WriteLine("  ?              help, this menu");
            Console.WriteLine("  cls            clear the screen");
            Console.WriteLine("  q              exit the application");
            Console.WriteLine("  write          write object to the container");
            Console.WriteLine("  writerange     write data to existing object in the container");
            Console.WriteLine("  writefile      write object using a file from the filesystem");
            Console.WriteLine("  read           read object from the container");
            Console.WriteLine("  readrange      read range from object in the container");
            Console.WriteLine("  readfile       read object to a file on the filesystem");
            Console.WriteLine("  remove         remove object from the container");
            Console.WriteLine("  exists         check if key exists in the container");
            Console.WriteLine("  list           list objects in the container");
        }
    }
}
