# Affqis (A Fancy Federated Query Interaction Server)

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

Here's an ugly hacked together example of chatting with the server using node + coffee + autobahn:

```coffee
autobahn = require 'autobahn'

connection = new autobahn.Connection(url: "ws://localhost:8080/affqis", realm: "hive")

connection.onopen = (session) ->
  connectionId = null
  session.call('connect', ["myuser", "my.hiveserver.com", 10000])
   .then((id) =>
     connectionId = id
     console.log("Connection id: #{id}")
     session.call('execute', [id, "select * from aschema.atable limit 10"]))
   .then((id) =>
     console.log("Execution id: #{id}")
     session.subscribe(id, (args) =>
       if args[0] == "finished"
         console.log "Done streaming results, closing connection #{connectionId}"
         session.call('disconnect', [connectionId]).then(console.log)
       else
         console.log(JSON.parse(args[1]))))

console.log("Connecting...")
connection.open()
```

And here's what the glorious output of such a thing might be:

```
Connecting...
Connection id: a259c6dd-793e-4408-93c5-9d094f546000
Execution id: execution.e843a88d_7dba_4e0f_91d5_aa12cf76e6a0
[ { value: 5, type: 'int', name: 'atable.id' },
  { value: 1, type: 'int', name: 'atable.game_id' },
  { value: 76168, type: 'int', name: 'atable.user_id' },
  ... and so on for each column ...]
[... next row and so on for each ...]

Done streaming results, closing connection a259c6dd-793e-4408-93c5-9d094f546000
true
```

Easy, right?