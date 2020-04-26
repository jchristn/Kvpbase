# Running Kvpbase in Docker
 
Getting an ```HttpListener``` application (such as Kvpbase or any application using Watson Webserver, HTTP.sys, etc) up and running in Docker can be rather tricky given how 1) Docker acts as a network proxy and 2) HttpListener isn't friendly to ```HOST``` header mismatches.  Thus, it is **critical** that you run your containers using ```--user ContainerAdministrator``` (Windows) or ```--user root``` (Linux or Mac) to bypass the ```HttpListener``` restrictions.  There are likely ways around this, but I have been unable to find one.  

## Before you Begin

As a persistent storage plaf stored within the container, will be lost once the container terminates.  Similarly, any metadata stored in a database would be lost if the database resides within the container.  Likewise, any object data stored within the filesystem of the container would also be lost should the container terminate.

As such, it is important that you properly deploy Kvpbase when using containers.  Use the following best practices.

### Copy in Node Configuration

The ```System.json``` file which defines the configuration for your node should be copied in as part of your ```Dockerfile```.  Do not allow it to be built dynamically.

Also, if you will be detaching (i.e. the ```-d``` flag in ```docker run```) be sure to set ```System.json``` ```EnableConsole``` to false and ```Syslog.ConsoleLogging``` to false.

Be sure to set your ```System.json``` ```Server.DnsHostname``` to ```*```.

### Use an External Database

Kvpbase relies on a database for storing object (and other) metadata.  While Kvpbase is capable of using Sqlite, Sqlite databases are stored on the filesystem within the container which will be lost once the container terminates.  Use an external database such as SQL Server, MySQL, or PostgreSQL.  Modify the ```System.json``` ```Database``` section accordingly. 

Valid values for ```Type``` are: ```MySql```, ```MsSql```, ```PgSql```, and ```Sqlite```
```
  "Database": {
    "Type": "MySql",  
    "Hostname": "[database server hostname]",
    "Port": 3306,
    "DatabaseName": "kvpbase",
    "InstanceName": null,
    "Username": "root",
    "Password": "[password]"
  }
```

### Use External Storage

Kvpbase stores object data on the filesystem.  While the filesystem location could be local, this is not recommended for containerized deployments because the container filesystem is destroyed when the container is terminated.  As such, it is recommended that Kvpbase rely on an NFS export or CIFS file share (or some other underlying shared storage) when deployed in a container.

Modify the ```System.json``` ```Storage``` section accordingly.  If necessary, modify the ```Dockerfile``` to issue the appropriate commands to establish the connection to the external or shared storage as part of the build process.
```
  "Storage": {
    "Directory": "[NFS, CIFS, or other shared or external storage mount]"
  }
```

## Steps to Run Kvpbase in Docker

1) View and modify the ```Dockerfile``` as appropriate for your application.

2) Execute the Docker build process:
```
$ docker build -t kvpbase -f Dockerfile .
```

3) Verify the image exists:
```
$ docker images
REPOSITORY                              TAG                 IMAGE ID            CREATED             SIZE
kvpbase                                 latest              047e29f37f9c        2 seconds ago       328MB
mcr.microsoft.com/dotnet/core/sdk       3.1                 abbb476b7b81        11 days ago         737MB
mcr.microsoft.com/dotnet/core/runtime   3.1                 4b555235dfc0        11 days ago         327MB
```
 
4) Execute the container:
```
Windows
$ docker run --user ContainerAdministrator -d -p 8000:8000 kvpbase 

Linux or Mac 
$ docker run --user root -d -p 8000:8000 kvpbase
```

5) Connect to Kvpbase in your browser: 
```
http://localhost:8000
```

6) Get the container name:
```
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED              STATUS              PORTS                    NAMES
3627b4e812fd        kvpbase             "dotnet Kvpbase.Stor…"   About a minute ago   Up About a minute   0.0.0.0:8000->8000/tcp   silly_khayyam
```

7) Kill a running container:
```
$ docker kill [CONTAINER ID]
```

8) Delete a container image:
```
$ docker rmi [IMAGE ID] -f
```

## Example System.json File

Notice in the ```System.json``` example provided below that:

- ```EnableConsole``` and ```Syslog.ConsoleLogging``` are false, so it is safe to detach using ```-d``` in ```docker run```
- An external ```MySql``` database is being used, so object metadata will persist even when the container is terminated
- External storage is used for object data, so object data will persist even when the container is terminated

```
{
  "EnableConsole": false,
  "Server": {
    "HeaderApiKey": "x-api-key",
    "MaxObjectSize": 2199023255552,
    "MaxTransferSize": 536870912,
    "Port": 8000,
    "DnsHostname": "*",
    "Ssl": false
  },
  "Storage": {
    "Directory": "/mnt/ExternalStorage/"
  },
  "Syslog": {
    "ServerIp": "127.0.0.1",
    "ServerPort": 514,
    "Header": "kvpbase",
    "MinimumLevel": 1,
    "ConsoleLogging": false,
    "FileLogging": true,
    "LogDirectory": "./Logs/"
  },
  "Database": {
    "Type": "MySql",  
    "Hostname": "10.104.12.16",
    "Port": 3306,
    "DatabaseName": "kvpbase",
    "InstanceName": null,
    "Username": "root",
    "Password": "[password]"
  },
  "Debug": {
    "Database": false,
    "HttpRequest": false
  }
}
```