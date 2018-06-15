using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kvpbase;

namespace Kvpbase
{
    class Program
    {
        static ContainerManager _Containers;
        static bool _RunForever = true;

        static void Main(string[] args)
        {
            _Containers = new ContainerManager("Containers.json", 3, 1);
             
            foreach (ContainerSettings currSettings in CreateSettings(10))
            {
                _Containers.Add(currSettings);
            }

            while (_RunForever)
            {
                Container container;
                string userName;
                string containerName;
                string key; 
                ContainerMetadata md;
                string data;
                string contentType;
                int position;
                int count;
                byte[] bytes;
                ErrorCode error;
                List<string> cached;

                string userInput = Common.InputString("ContainerManager [? for help] >", null, false);

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
                        userName = Common.InputString("User:", null, false);
                        containerName = Common.InputString("Container:", null, false);
                        key = Common.InputString("Key:", null, false);
                        data = Common.InputString("Data:", null, true);
                        contentType = Common.InputString("Content Type:", "application/octet-stream", false);

                        if (!_Containers.GetContainer(userName, containerName, out container))
                        {
                            Console.WriteLine("Unknown container");
                            break;
                        }

                        if (container.WriteObject(key, contentType, Encoding.UTF8.GetBytes(data), out error))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Error: " + error.ToString());
                        }
                        break;

                    case "writerange":
                        userName = Common.InputString("User:", null, false);
                        containerName = Common.InputString("Container:", null, false);
                        key = Common.InputString("Key:", null, false);
                        position = Common.InputInteger("Position:", 0, true, true);
                        data = Common.InputString("Data:", null, true);

                        if (!_Containers.GetContainer(userName, containerName, out container))
                        {
                            Console.WriteLine("Unknown container");
                            break;
                        }

                        if (container.WriteRangeObject(key, position, Encoding.UTF8.GetBytes(data), out error))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Error: " + error.ToString());
                        }
                        break;

                    case "read":
                        userName = Common.InputString("User:", null, false);
                        containerName = Common.InputString("Container:", null, false);
                        key = Common.InputString("Key:", null, false);

                        if (!_Containers.GetContainer(userName, containerName, out container))
                        {
                            Console.WriteLine("Unknown container");
                            break;
                        }

                        if (container.ReadObject(key, out contentType, out bytes, out error))
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

                    case "readrange":
                        userName = Common.InputString("User:", null, false);
                        containerName = Common.InputString("Container:", null, false);
                        key = Common.InputString("Key:", null, false);
                        position = Common.InputInteger("Position:", 0, true, true);
                        count = Common.InputInteger("Count:", 0, true, true);

                        if (!_Containers.GetContainer(userName, containerName, out container))
                        {
                            Console.WriteLine("Unknown container");
                            break;
                        }

                        if (container.ReadRangeObject(key, position, count, out contentType, out bytes, out error))
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
                        userName = Common.InputString("User:", null, false);
                        containerName = Common.InputString("Container:", null, false);
                        key = Common.InputString("Key:", null, false);

                        if (!_Containers.GetContainer(userName, containerName, out container))
                        {
                            Console.WriteLine("Unknown container");
                            break;
                        }

                        if (container.RemoveObject(key, out error))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Error: " + error.ToString());
                        }
                        break;

                    case "exists":
                        userName = Common.InputString("User:", null, false);
                        containerName = Common.InputString("Container:", null, false);
                        key = Common.InputString("Key:", null, false);

                        if (!_Containers.GetContainer(userName, containerName, out container))
                        {
                            Console.WriteLine("Unknown container");
                            break;
                        }

                        Console.WriteLine("Exists: " + container.Exists(key));
                        break;

                    case "list":
                        userName = Common.InputString("User:", null, false);
                        containerName = Common.InputString("Container:", null, false);

                        if (!_Containers.GetContainer(userName, containerName, out container))
                        {
                            Console.WriteLine("Unknown container");
                            break;
                        }

                        md = container.Enumerate(null, null, null, null);
                        if (md != null)
                        {
                            Console.WriteLine(Common.SerializeJson(md, true)); 
                        }
                        else
                        {
                            Console.WriteLine("None");
                        }
                        break;

                    case "cached":
                        cached = _Containers.CachedContainers();
                        if (cached != null)
                        {
                            Console.WriteLine(Common.SerializeJson(cached, true));
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
            Console.WriteLine("  read           read object from the container");
            Console.WriteLine("  readrange      read range from object in the container");
            Console.WriteLine("  remove         remove object from the container");
            Console.WriteLine("  exists         check if key exists in the container");
            Console.WriteLine("  list           list objects in the container");
            Console.WriteLine("  cached         list the names of container objects in RAM cache");
        }

        static List<ContainerSettings> CreateSettings(int count)
        {
            List<ContainerSettings> ret = new List<ContainerSettings>();

            for (int i = 0; i < count; i++)
            {
                ContainerSettings settings = new ContainerSettings("Default", "Container" + i.ToString(), "./");
                ret.Add(settings);
            }

            return ret;
        }
    }
}
