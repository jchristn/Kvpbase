# Kvpbase Storage Server

Object Storage Platform in C#

Kvpbase Storage Server is a RESTful object storage platform written in C# and available under the MIT license.  

## Help and Feedback
First things first - do you need help or have feedback?  Contact me at joel at maraudersoftware.com dot com or file an issue here!  We would love to get your feedback to help make our product better.

## Documentation
Please read our documentation on Github: 

## New in v2.0.1
- Our first open source release!

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

## Getting Started
It's easy to get started with Kvpbase Storage Server.  Clone to your local machine and build, and then execute the binary.  A setup class is included which will produce the requisite configuration files and directories.

## Running under Mono
Kvpbase is running in Mono in many customer environments.  You must first use the Mono ahead-of-time (AOT) compiler for proper operation, otherwise you will encounter problems.  Further, your configured hostame (which is used to start the HTTP listener) MUST be a DNS address (Mono requires that the host header in the incoming HTTP request match the listener).
```
mono --aot=nrgctx-trampolines=8096,nimt-trampolines=8096,ntrampolines=4048 --server StorageServer.exe
mono --server StorageServer.exe
```

## Version History
Notes from previous versions (starting with v2.0.1) will be moved here.
