Reactive Scala driver for Couchbase 
===================================

Note: this project is in very early stage, it only contains experimental implementation of couple basic features.

### Goal
The goal of this project is to create native scala client library for Couchbase.

#### Advantages over java client
1. Using scala futures for better integration
2. Using Iteratee library for view results - query results are parsed asynchronously, and can be handled reactively 

##### Features
* Async - all operations will be asynchronous, using Scala futures and Iteratees
* Non blocking - all IO will be implemeted using Akka IO and Spray Client
