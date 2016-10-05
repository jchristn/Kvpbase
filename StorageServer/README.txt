Thank you for choosing kvpbase!  Run StorageServer.exe and we will create the 
necessary configuration files and directories for system operation.  

Mac and Linux users, perform steps 1, 2, and 3.
Windows users, proceed to step 3.

1) Install Mono:
- Linux : $ sudo apt-get install mono-complete
- Mac   : download Mono MRE from http://www.mono-project.com/download/

2) Run the Mono Ahead of Time compiler:
$ su
# mono --aot=nrgctx-trampolines=8096,nimt-trampolines=8096,ntrampolines=4048 --server storage-server.exe

3) Run kvpbase as root/administrator.
- Windows : c:\kvpbase> storage-server.exe
- Linux   : $ sudo mono --server StorageServer.exe
- Mac     : $ sudo mono --server StorageServer.exe

More complete Mono installation documentation can be found at
http://www.mono-project.com/docs/getting-started/install/linux/

Prior to using the system and after installation, we recommend that you modify 
the System.json file to change the admin API key, set the port number, and 
enable SSL.

