# Kvpbase Storage Server

Object Storage Platform in C#

Kvpbase Storage Server is a RESTful object storage platform written in C# and available under the MIT license.  

![alt tag](https://github.com/kvpbase/storage-server/blob/master/assets/diagram.png)

## Help and Feedback
First things first - do you need help or have feedback?  Contact me at joel at maraudersoftware.com dot com or file an issue here!  We would love to get your feedback to help make our product better.

## Initial Setup

The binaries for Kvpbase can be created by compiling from source or using the pre-compiled binaries found in StorageServer\bin\release (I intentionally did not .gitignore these files).

In Windows environments, run StorageServer.exe to create the requisite JSON configuration files.

In Linux and Mac environments, first run the Mono AOT prior to running StorageServer.exe in Mono.  It is recommended that you use the ```--server``` flag for both the AOT and while running under Mono.
```
sudo mono --aot=nrgctx-trampolines=8096,nimt-trampolines=8096,ntrampolines=4048 --server StorageServer.exe
sudo mono --server StorageServer.exe
```

In Linux and Mac environments, the listener (specifically the ```DnsHostname``` defined within each Linux and Mac node within Topology.config) MUST be a hostname or IP address.  Incoming requests must have a HOST header matching this exact value.

## Configuration Files

The following configuration files are created by setup process:

- System.json - the main system configuration file
- UserMaster.json - containing the user objects
- ApiKey.json - containing API keys associated with users
- ApiKeyPermission.json - permissions associated with API keys
- Topology.json - node definitions

## Your First GET Requests

By default, data is stored within ```./storage/<user_guid>```.  The setup process will create a series of sample files within the directory which can be retrieved:

- GET http://localhost:8080/default/hello.html?x-api-key=default
- GET http://localhost:8080/default/hello.txt?x-api-key=default
- GET http://localhost:8080/default/hello.json?x-api-key=default

## Creating Your First Object

To create your first object, call ```PUT /<user_guid>/<filename>```.  A simple cURL example is shown here.
```
$ curl -X PUT -d "My first file!" "http://localhost:8080/default/firstfile.txt?x-api-key=default"
```

The response is simply a text URL with a 200/OK.
```
http://localhost:8080/default/firstfile.txt
```

Then retrieve it:
```
$ curl "http://localhost:8080/default/firstfile.txt?x-api-key=default"
```

The result is a 200/OK with your data:
```
My first file!
```

## Deleting Your First Object

To delete your first object, call ```DELETE /<user_guid>/<filename>```.  A simple cURL example is shown here.
```
$ curl -X DELETE "http://localhost:8080/default/firstfile.txt?x-api-key=default"
```

The response is simply a 200/OK.

## Documentation
Please read our documentation on Github: https://github.com/kvpbase/storage-server/wiki/Getting-Started
And our API documentation here: http://www.kvpbase.com/docs

## New in v2.1.0
- Performance improvements
- Simplified default HTML pages
- Reduced CPU utilization
- Single instancing of certain classes to reduce overhead

## Use Cases
Core use cases for Kvpbase Storage Server:
- Object storage - create, read, update, delete, search objects using HTTP
- Container storage - create, read, update, delete, search containers using HTTP
- Primary storage for objects - range read, range write, and append support
- Secure storage - automatic encryption using either symmetric key or one-time-use keys for PCI, HIPAA, or any PII
- Scalable storage - multi-node support, sync and async replication, bunkering to another deployment
- Filesystem gateway - RESTful access to existing SAN/DAS (block with filesystem) or NAS (fileshares via CIFS, NFS)

## SDKs
Numerous SDKs are already available for Kvpbase Storage Server: https://github.com/kvpbase/

## Version History
Notes from previous versions (starting with v2.0.1) will be moved here.
- First open source release
- Massive refactor