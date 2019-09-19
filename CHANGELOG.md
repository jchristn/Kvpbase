# Change Log

## Current Version

v4.0.1

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

## Previous Versions
 
v3.x

- Enumeration support using object key prefixes
- Support for stream processing instead of byte arrays (better memory usage, support for large objects)
- Server settings MaxObjectSize and MaxTransferSize
- Temporary files manager to overcome byte array size limitations (now using streams)
- Removed task manager and enhanced resync manager
- Updated dependencies
- Retarget to .NET Core 2.2 and .NET Framework 4.6.1
- Updated dependencies
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
