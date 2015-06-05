# Affqis (A Fancy Federated Query Interaction Server)

[![Build Status](https://travis-ci.org/scopely/affqis.svg?branch=travis)](https://travis-ci.org/scopely/affqis)

Affqis is a unified JDBC bastion server, using WAMP as a means of communicating with clients.
The idea is that you talk to it to connect to a server and run queries and it parses and
streams the results back to you as JSON.

Affqis only supports HiveServer2 at the moment.

## Usage

```
$ sbt pack
$ cd target/pack
$ bin/affqis
```

You can configure the netty server with environment variables:

```
HOST=0.0.0.0
PORT=8080
```