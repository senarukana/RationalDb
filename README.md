#RationalDB
RationalDB is a trial distributed database middleware for k/v storage engine based on Vitess.  
Currently, a large part of code is borrowed from vitess. The architecture and concepts are almost the same to it. If you don't know about it, I strongly recommend you to see the documention of vitess first. The middleware itself is really awesome.

Now, RationalDB supports RocksDB and LevelDB. It aims to provide eaier access to this storage engine without sacrifising the initial motivation and efficiency. It provides network, SQL parser and excution module for them. 

>Notice that as k/v engine is designed to handle simple use case, RationalDB willonly supports parts of sql. And the following functions will not be considered.
 
1. Join, CTE, GROUP, complicated searches(search condition should only depend on the column that are indexed). 
 
2. Transaction.


# Warning
RationalDB is only a trial experiment and not tested comprehensively. It is inappropriate use it in real case.
RationalDB is currently under highly development, and the api may change frequently.

# Getting started
1. Make sure you have leveldb and rocksdb installed.
2. Make sure you have go environment and GOPATH is set correctly.
3. Make sure you have levigo and ratgo. The go wrapper for two of them above.
4. Install RationalDB

	go get github.com/senarukana/RationalDb
	
	go install github.com/senarukana/RationalDb
	
5. run the following command to test if the environment is set correctly.
 
    go get github.com/senarukana/RationalDB/vt/kvengine
    
	if evertthing is ok, it will return nothing. Otherwise, you need to check the above for steps.

>Notice: I agree that it is really hard to install it. I will try to fix it soon.

# Usage
The server and client program is in the path of /cmd. You need compile it first:

	go install github.com/senarukana/RationalDb/cmd/vtocc
	go install github.com/senarukana/RationalDb/cmd/vtclient

vtocc is the server.
vtclient is the client.

Launch the server:

	./vtserver --ip 127.0.0.1 --port 1234
the default server port is 6510.

vtclient is an interactive shell client. 
	
	./vtclient
you can specify server address by: --server, like:localhost:1234

get full information of execution: --verbose

Client will connect the server automatically.

## SQL
###OverView
All of the sql supported in RationalDB is a subset of ansi sql and has a litter difference.

1. No JOIN, CTE, Complex searches(search condition should only depend on the column that are indexed).
2. No Transaction. 
3. Currently it doesn't have the term SCHEME, it only have the term Table. All the table will be stored together.
4. No sql func is provided.

### TABLE LIMITATION
1. It only has 4 types : int, double, text, varchar(size)
2. It doesn't support constraints, check and foreign key.
3. A table without primary key is not allowed.

### INDEX LIMITATION
1. non-unique column is not allowed to create index

Then, everything else is the same with sql.

### Example
Let's create a test tabase

	RationalDB> CREATE TABLE student (stuid int PRIMARY KEY AUTO INCREMENT, \
	       ...>  name varchar(50) NOT NULL UNIQUE, \
	       ...>  age int NOT NULL, description text);
	# create index on age
	RationalDB> CREATE INDEX idx_student_name(name); // OK, index on unique column
	RationalDB> CREATE INDEX idx_student_age(age); // NOT OK, index on non-unique column.
	# insert some data
	RationalDB> INSERT INTO student VALUES('ted', 24, "bupt");
	RationalDB> INSERT INTO student VALUES('huzheng', 23, "hunnu");
	RationalDB> INSERT INTO student (name, age) VALUES('yao', 50); // OK, no description 
	# select
	RationalDB> SELECT * FROM student; // OK, get all the data
	RationalDB> SELECT name FROM student WHERE stuid < 3; // OK, Primary Index on stuid
	RationalDB> SELECT age FROM student WHERE name='ted'; // OK, has index on name!
	RationalDB> SELECT name FROM student WHERE age=24; // NOT OK, no index on age.
 
