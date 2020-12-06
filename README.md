udfs (ucz distributed file system)
=======

A distributed file system, based on hdfs/gfs. 
Just for fun and learning.

![CI](https://github.com/radogost/udfs/workflows/CI/badge.svg?branch=master)

Work is still in progress, a lot of things are missing.

If you want to try it out, the easiest way it to spin up docker containers (`docker-compose up`), and bash into a running container (`docker exec -it <id> bash`).

Inside a running container, you can:
1. List datanodes `shell report`
2. ls `shell ls /`
3. mkdir `shell mkdir /foo/bar/baz`
4. Upload a file `shell put build.rs /build.rs`
5. Download a file `shell get /build.rs ./temp`



To spin up nodes yourself:
##### Spin up a namenode
`UDFS_CONFIG_FILE=/path/to/config.toml cargo run --bin namenode`

##### Spin up a datanode
`UDFS_CONFIG_FILE=/path/to/config.toml cargo run --bin datanode localhost:42001`

##### Control logging
`RUST_LOG=udfs=debug ...`

