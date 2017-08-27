
## Dependency

- [protobuf-2.6.1](https://github.com/google/protobuf/releases/tag/v2.6.1)
- [leveldb-1.20](https://github.com/google/leveldb/releases/tag/v1.20)
- [snappy-1.1.6](https://github.com/google/snappy/releases/tag/1.1.6)
- [libco](https://github.com/tencent/libco)
- [googletest](https://github.com/google/googletest)

There are some shells for installing dependency in dependency_tools.

## Build

```bash
$ git submodule init
$ git submodule update

$ mkdir build
$ cd build
$ cmake ..
$ make -j24
```

## [Implement Details](./impl_note.md)

