# Pystardog 2

Welcome to the new and improve pystardog api such as 

* Improved validation across
* Improved connection management
* Improved Transaction support
  * Ability to `add/remove/clear` without worring to create or `commit` a new transaction
  * Auto-rollback transaction when an issue occurs
* Improved development environment
  * Method Signature integration (we recommend using Pycharm)
  * Easier to run integration test
  * Now has some unit test. 

It was decided to focus on improvement rather than backward compatibility, however pystardog 2.0
is in  different namespace, and therefore both version can be used  in parallel while you 
migrate. 


# Requirement

PyStardog 2 was designed for Python 3

# Connection Management

In pystardog 2 we completely reworked how connection management work to register your connection 
info only once in most case, but have finer control when needed. 

## Set the connection details for all api call. 

The simplest case is if you want to connect with the defaul, simply start using the API. However 
since we recommend changing the credential and using https, the next simplest case is to simply 
register with the API

```
connectionDetails = {
"endpoint": "https://localhost:5821",
"username": "administator",
"password": "myNewPwd",
}

client = client.Client(**connectionDetails)

Base.set_client(client)
```

Now regardless of which object you instantiate, they will use this connection information. 

## Override the connection details for a specific instance. 

In some case you may need to connect to more than one server. For example, let assume 

* You will read data from one server, 
* And write to another. We could 

```
readClient = client.Client(**readConnectionDetails)

qm = QueryManager('myreaddb')
qm.set_client(readClient)

#read data 

tx = Transaction('mywritedb') 

#write data using default client
```

# QueryManager

The `QueryManager` allow you to perform atomic queries that used to be achieved using `Connection`. The query manager 
will even allow you to call `add`, `remove` and `clear` without you having you worry about transaction details. In other 
word the API will automatically create the transaction and commit it for you transparently. 

For example, if you only plan to to add a single file, it can be easily be acheived with 

```
qm = QueryManager('db')
qm.add(GraphFile('input.ttl'))
```

Method signature include types, which make using the API much easier. For example, you no longer need to figure our which 
content_type the method `query`, `ask`, `paths`, `update` and `graph` support, since they each have their own Enumeration.

It also now possible to list current transaction using
```
qm = QueryManager('db')
txs = qm.list_tx
```
which will return a list of `Transaction` objects. 

# Transaction 

A `Transaction` object is in fact a `QueryManager`, in other word it allow you to perform all the actions you would in a 
`QueryManager` within a transaction. There are different entry point to start a transaction

```
tx = Transaction('db')
#or 
qm = QueryManager('db')
tx = qm.begin_tx()
#or 
db = Database('db')
tx = db.begin_tx()
```

Once you have a transaction you can perform action as you would with the `QueryManager`, though you are responsible for 
calling `commit` once you are done. 

A great improvement over `Transaction` is that by default you no longer need to worry about rollback, as the API will 
automatically do it when any error is encountered. If you do not wish for the API to rollback automatically you can disable
it as such

```
tx = Transaction('db', auto_rollback=False)
```



