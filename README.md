# Kvpbase Storage Server

Object Storage Platform in C#

Kvpbase Storage Server is a RESTful object storage platform written in C# and available under the MIT license.  

As of v3.2, Kvpbase is targeted to both .NET Core 2.2 and .NET Framework 4.6.1.

![alt tag](https://github.com/kvpbase/storage-server/blob/master/assets/diagram.png)

## Help and Feedback

First things first - do you need help or have feedback?  Contact me at joel dot christner at gmail dot com or file an issue here. 

## Initial Setup

The binaries for Kvpbase can be created by compiling from source or using the pre-compiled binaries found in ```StorageServer\bin\release``` (I intentionally did not ```.gitignore``` these files).

In Windows environments, run ```StorageServer.exe``` to create the requisite JSON configuration files.

In Linux and Mac environments, first run the Mono AOT prior to running StorageServer.exe in Mono.  It is recommended that you use the ```--server``` flag for both the AOT and while running under Mono.
```
sudo mono --aot=nrgctx-trampolines=8096,nimt-trampolines=8096,ntrampolines=4048 --server StorageServer.exe
sudo mono --server StorageServer.exe
```

In Linux and Mac environments, the listener (```Nodes.Http.DnsHostname``` in the ```Topology.json``` file) MUST be a hostname or IP address.  Incoming requests must have a HOST header matching this exact value.  If it does not match, you will receive a ```400/Bad Request```.

## Configuration Files

The following configuration files are created by setup process:

- System.json - the main system configuration file
- UserMaster.json - containing the user objects
- ApiKey.json - containing API keys associated with users
- ApiKeyPermission.json - permissions associated with API keys
- Topology.json - node definitions
- Container.json - storage containers mapped to users and associated metadata

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

To see the contents of your container, call ```GET /[userguid]/[containername]?_container=true```.
```
$ curl "http://localhost:8000/default/default?_container=true"
```

## Deleting Your First Object

To delete your first object, call ```DELETE /[userguid]/[containername]/[objectkey]```.  A simple cURL example is shown here.
```
$ curl -X DELETE "http://localhost:8000/default/default/firstfile.txt?x-api-key=default"
```

The response is simply a 200/OK.

## Documentation

Please read our documentation on Github [https://github.com/kvpbase/storage-server/wiki/Getting-Started] and our API documentation [http://www.kvpbase.com/docs].  Note that documentation updates to 3.0.0 are in progress.

## New in v3.1.1

- Retarget to .NET Core 2.2 and .NET Framework 4.6.1

## Compatibility 

### With 3.0.x

- 3.1.0 is compatible with 3.0.x with minor changes to each container SQLite database.  Simply add the 'Tags' field to the 'Objects' table, with field type ```VARCHAR(256)```.

### With 2.x.x

- 3.x.x is fundamentally incompatible with 2.x.x (hence the major version change)
- URL structure explicitly fixed in format with 3.x.x (i.e. /[userguid]/[containername]/[objectkey])
- Bunkering temporarily removed (this will be reintroduced)
- Branch ```release-2.1``` has been preserved for those that need it: https://github.com/kvpbase/storage-server/tree/release-2.1

## Use Cases

Core use cases for Kvpbase Storage Server:

- Object storage - create, read, update, delete, search objects using HTTP
- Container storage - create, read, update, delete, search containers using HTTP
- Primary storage for objects - range read, range write, and append support
- Secure storage - automatic encryption using either symmetric key or one-time-use keys for PCI, HIPAA, or any PII
- Scalable storage - multi-node support, sync and async replication, bunkering to another deployment
- Filesystem gateway - RESTful access to existing SAN/DAS (block with filesystem) or NAS (fileshares via CIFS, NFS)

## SDKs and Sample Scripts

Numerous SDKs and sample scripts are already available for Kvpbase Storage Server: https://github.com/kvpbase/.  Need an SDK for a different language?  Let me know!  Currently, SDKs are available in:

- C# - https://github.com/kvpbase/sdk-csharp
- Javascript - https://github.com/kvpbase/sdk-js
- Java - https://github.com/kvpbase/sdk-java
- Python - https://github.com/kvpbase/sdk-python
- cURL - https://github.com/kvpbase/curl-scripts

## Version History

Notes from previous versions (starting with v2.0.1) will be moved here.

v3.x

- Bugfix for retrieving request metadata vs container/object metadata (new querystring parameter)
- Support for object tags (on create/POST, on edit/PUT, and for enumeration)
- Significant performance improvements
- Major refactor in preparation of new features, better code manageability
- Container class now used for metadata management, eliminating dependency on filesystem for enumeration and encapsulation for extended metadata
- Support for larger number of objects per container
- Better support for public containers (read, write)
- Streamlined replication and messaging based on persistent TCP sockets amongst nodes (instead of using HTTP APIs)
- Container resync support (useful when replacing failed node or populating container contents on another node)

v2.1.x

- Version 2.1 can be found in the release-2.1 branch: https://github.com/kvpbase/storage-server/tree/release-2.1
- Performance improvements
- Simplified default HTML pages
- Reduced CPU utilization
- Single instancing of certain classes to reduce overhead

v2.0.x

- First open source release
- Massive refactor