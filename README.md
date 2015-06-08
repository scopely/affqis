# Affqis (A Fancy Federated Query Interaction Server)

[![Build Status](https://travis-ci.org/scopely/affqis.svg)](https://travis-ci.org/scopely/affqis)

Affqis is a unified JDBC bastion server, using WAMP as a means of communicating
with clients. The idea is that you talk to it to connect to a server and run queries
and it parses and streams the results back to you as JSON.

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

You'll need a wamp library to connect to this guy. If you're using Java or
Scala, I suggest Jawampa, the library I use for Affqis itself. It's the most
complete Java-based client/router I've been able to find, and Scala has 
nothing at all that's maintained. [Autobahn](http://autobahn.ws/) offers 
great solutions for Python and Javascript/Node, and an example of an affqis
client can be found in Scopely's [sqlquest](https://github.com/scopely/affqis)
project, in
[affqis.coffee](https://github.com/scopely/sqlquest/blob/master/src/affqis.coffee).

Affqis WAMP procedures take keyword arguments for all procedures and events.
Affqis's little protocol has a flow that looks like the following:

1) Connect to the Affqis websocket at `ws://$HOST:$PORT/affqis`.
2) Configure for which realms (databases correspond to realms) you want to access.
   Right now, only hive is supported.
3) Call the WAMP `connect` procedure with the required parameters for the database
   you're connecting to. Standard keyword parameters are:
   
   * `user` -> String
   * `host` -> String
   * `port` -> Integer
   * `database` -> String
   * `password` -> String
   
   Not all realms will require all of these parameters and some may take extras,
   but you can expect these standard parameters to at least be the correct name
   and type.
   
   The result of this procedure will be a UUID, and this unique ID corresponds to
   your JDBC connection object internally. Hold on to it.
4) Call the `execute` procedure. This guy takes the following keyword parameters:

   * `connectionId` -> String | ID returned from `connect`
   * `sql` -> String | A single SQL statement with no semi-colon. See Scopely's
                       [sqlsplit](https://github.com/scopely/sqlsplit) server for
                       a tool to split queries if needed.
   
   The result of this call will be two strings: an event to subscribe to for the
   query results, and a procedure to call once you have successfully established a
   subscription. This is to make sure you don't miss any result events.
   
   Once you've called the callback procedure, you'll begin receiving events. These
   will either be two strings, `"row"` and a string-serialized json object with row
   data that you should parse and work with, or just "finished" when there are no
   more rows to stream.
5) When you're finished with your connection you should call the `disconnect` procedure
   with your `connectionId`. It'll return `true` if successful. For the time being,
   connections will not get cleaned up without an explicit disconnect. They'll remain in
   memory until a restart or a shutdown. We should give these an idle timeout.

There is an ugly example of this whole process in the tests, but
[sqlquest](https://github.com/scopely/sqlquest/blob/master/src/affqis.coffee) remains
the best example.