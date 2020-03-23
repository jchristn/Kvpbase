using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging; 
using WatsonWebserver;
 
using Kvpbase.StorageServer.Classes.Handlers;  

namespace Kvpbase.StorageServer.Classes.Managers
{
    internal class ConsoleManager
    { 
        private bool _Enabled { get; set; }
        private Settings _Settings { get; set; }   
        private LoggingModule _Logging { get; set; }  
        private ContainerManager _ContainerMgr { get; set; } 
        private ObjectHandler _Objects { get; set; }

        internal ConsoleManager(
            Settings settings,
            LoggingModule logging,   
            ContainerManager containerMgr, 
            ObjectHandler objects)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings)); 
            if (logging == null) throw new ArgumentNullException(nameof(logging));   
            if (containerMgr == null) throw new ArgumentNullException(nameof(containerMgr)); 
            if (objects == null) throw new ArgumentNullException(nameof(objects)); 

            _Enabled = true;

            _Settings = settings;
            _Logging = logging;   
            _ContainerMgr = containerMgr; 
            _Objects = objects; 
        }

        internal void Worker()
        {
            string userInput = "";

            while (_Enabled)
            {
                Console.Write("Command (? for help) > ");
                userInput = Console.ReadLine();

                if (userInput == null) continue;
                switch (userInput.ToLower().Trim())
                {
                    case "?":
                        Menu();
                        break;

                    case "c":
                    case "cls":
                    case "clear":
                        Console.Clear();
                        break;

                    case "q":
                    case "quit":
                        _Enabled = false; 
                        break;
                           
                    default:
                        Console.WriteLine("Unknown command.  '?' for help.");
                        break;
                }
            }
        }
         
        private void Menu()
        {
            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("  ?                         help / this menu");
            Console.WriteLine("  cls / c                   clear the console");
            Console.WriteLine("  quit / q                  exit the application");     
            Console.WriteLine("");
            return;
        } 
    }
}
