# Kvpbase Storage Server

```
   _             _
  | |____ ___ __| |__  __ _ ___ ___
  | / /\ V / '_ \ '_ \/ _` (_-</ -_)
  |_\_\ \_/| .__/_.__/\__,_/__/\___|
           |_|

```

[![StackShare](http://img.shields.io/badge/tech-stack-0690fa.svg?style=flat)](https://stackshare.io/jchristn/kvpbase)

![alt tag](https://github.com/kvpbase/storage-server/blob/master/assets/diagram.png)

Scalable, simple RESTful object storage platform, written in C#

## Introducing v4.0

We're happy to release v4.0, bringing a wealth of new features, optimizations, and fixes to enable you to deploy a more performant and scalable object storage platform.  We have a healthy pipeline of features and capabilities we plan to bring to Kvpbase.  If you have any suggestions, please file an issue and let us know!

### New Features

- Integration with external databases for configuration, thereby enabling consistency across nodes, simplicity, scale-out, and reducing JSON files
- Removed Kvpbase.Core library (merged into StorageServer and KvpbaseSDK directly) for simplicity
- Reduced ```System.json``` file for simplicity
- Async APIs for better performance and scale
- New search API at ```PUT /<container>/?_search``` using an ```EnumerationFilter``` allowing filtering by timestamps, prefix, content-type, MD5, and tags
- Certain querystring elements no longer require ```=true```
- Support for hierarchical structures within a container using zero-byte objects (folders) and objects with ```/``` in the name
- Each object now stored using a unique identifier to enable support for versioning (future)
- Optimized memory utilization with large objects (internal optimizations now rely on streams)
- Support for object tagging and extensible key-value pair metadata 
- Enhanced container statistics
- Dependency updates
- Retarget to both .NET Core 2.2 and .NET Framework 4.6.1

### Fixes

- Major code refactor and simplification
- Fixed issues associated with object range reads
- Fixed issues associated with container cleanup on delete
 
## Help and Feedback

First things first - do you need help or have feedback?  File an issue here!

## Initial Setup

The binaries for Kvpbase can be created by compiling from source or using the pre-compiled binaries found in ```Kvpbase.StorageServer\bin\release\[framework]\``` (I intentionally did not ```.gitignore``` these files).  Executing the binaries will create the requisite configuration files and database tables.

Important: you MUST create the database to be used by Kvpbase prior to running the application.  Kvpbase will automatically create the tables for you.

### Windows (.NET Framework)
```
> Kvpbase.StorageServer.exe
```

### Windows, Linux, Mac (.NET Core)
```
$ dotnet Kvpbase.StorageServer.dll
```

### Mono
```
$ sudo mono --aot=nrgctx-trampolines=8096,nimt-trampolines=8096,ntrampolines=4048 --server Kvpbase.StorageServer.exe
$ sudo mono --server Kvpbase.StorageServer.exe
```

When specifying the listener hostname ```Server.DnsHostname``` in the ```system.json``` file, follow these rules:
- If you are using an IP address that listens on any interface such as ```0.0.0.0```, ```*```, or ```+```, Kvpbase must be run using elevated privileges
- If using any other IP address or DNS name, the HOST header on incoming requests *MUST* match the value for this parameter

## Your First GET Requests

By default, data is stored within ```./storage/[userguid]/[containername]```.  The setup process will create a series of sample files within the ```default``` user's container, also named ```default```, which is configured for public/unauthenticated read access:

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
 
## Compatibility with Previous Versions

Kvpbase v4.x interacts with an external database whereas previous versions relied on an internally-managed SQLite database.  If you wish to migrate from a previous version to v4.x, please file an issue and we will prioritize documenting the new database schema so you can better script the migration.

## Use Cases

Core use cases for Kvpbase Storage Server:

- Object storage - create, read, update, delete, search objects using HTTP
- Container storage - create, read, update, delete, search containers using HTTP
- Primary storage for objects - range read, range write, and append support
- Scalable storage - multi-node scale-out support using shared backend disk storage 
- Filesystem gateway - RESTful access to existing SAN/DAS (block with filesystem) or NAS (fileshares via CIFS, NFS)

## SDKs and Sample Scripts

Numerous SDKs and sample scripts are already available for Kvpbase Storage Server: https://github.com/kvpbase/.  Need an SDK for a different language?  Let me know!  Currently, SDKs are available in:

- C# - https://github.com/kvpbase/sdk-csharp
- Javascript - https://github.com/kvpbase/sdk-js
- Java - https://github.com/kvpbase/sdk-java
- Python - https://github.com/kvpbase/sdk-python
- cURL - https://github.com/kvpbase/curl-scripts

## Version History

Refer to CHANGELOG.md for version history.
