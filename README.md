# Kvpbase Storage Server
 
[![StackShare](http://img.shields.io/badge/tech-stack-0690fa.svg?style=flat)](https://stackshare.io/jchristn/kvpbase)

![alt tag](https://github.com/kvpbase/storage-server/blob/master/assets/diagram.png)

Scalable, simple RESTful object storage platform, written in C#

## New in v4.2

- Code manageability improvement, migration to ORM
- Dependency updates
- More complete Postman collection
- Lock management APIs

## Help and Feedback

First things first - do you need help or have feedback?  File an issue here!

## Initial Setup

The binaries for Kvpbase can be created by compiling from source or using the pre-compiled binaries found in ```Kvpbase.StorageServer\bin\release\net5.0\``` (I intentionally did not ```.gitignore``` these files).  Executing the binaries will create the requisite configuration files and database tables.

```
$ dotnet Kvpbase.StorageServer.dll
```

By default, Kvpbase will listen on ```localhost``` and only handle requests from the local machine.  If you wish to change this, modify ```Server.DnsHostname``` in the ```system.json``` file.  When modifying this value, follow these rules:

- If you are using an IP address that listens on any interface such as ```0.0.0.0```, ```*```, or ```+```, Kvpbase must be run using elevated privileges
- If using any other IP address or DNS name, the HOST header on incoming requests *MUST* match the value for this parameter

## Your First GET Requests

By default, data is stored within ```./Storage/[userguid]/[containername]```.  The setup process will create a series of sample files within the ```default``` user's container, also named ```default```, which is configured for public/unauthenticated read access:

- GET http://localhost:8000/default/default/hello.html 
- GET http://localhost:8000/default/default/hello.txt 
- GET http://localhost:8000/default/default/hello.json 

## Creating Your First Object

To create your first object, call ```POST /[userguid]/[containername]/[objectkey]```.  A simple cURL example is shown here.
```
$ curl -X POST -d "My first object!" "http://localhost:8000/default/default/firstfile.txt?x-api-key=default"
```

The response is simply a 200/OK. 

Then retrieve it:
```
$ curl http://localhost:8000/default/default/firstfile.txt
```

The result is a 200/OK with your data:
```
My first object!
```

## Enumerate the Container

To see the contents of your container, call ```GET /[userguid]/[containername]```.
```
$ curl "http://localhost:8000/default/default"
```

## Deleting Your First Object

To delete your first object, call ```DELETE /[userguid]/[containername]/[objectkey]```.  A simple cURL example is shown here.
```
$ curl -X DELETE "http://localhost:8000/default/default/firstfile.txt?x-api-key=default"
```

The response is simply a 200/OK.

## Documentation

Please visit our documentation [https://github.com/kvpbase/storage-server/wiki] for details on APIs, configuration files, deployment scenarios, and more. 
  
## Use Cases

Core use cases for Kvpbase Storage Server:

- Object storage - create, read, update, delete, search objects using HTTP
- Container storage - create, read, update, delete, search containers using HTTP
- Primary storage for objects - range read, range write, and append support
- Scalable storage - multi-node scale-out support using shared backend disk storage 
- Filesystem gateway - RESTful access to existing SAN/DAS (block with filesystem) or NAS (fileshares via CIFS, NFS)

## Version History

Refer to CHANGELOG.md for version history.
