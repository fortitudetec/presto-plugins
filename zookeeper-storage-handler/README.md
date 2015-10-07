# ZooKeeper Storage Handler

This storage handler connects ZooKeeper to Presto for queries ZooKeeper information via SQL.

### Building

Maven package command:

    mvn package

After cloning and building extract the `zookeeper-storage-handler-*-plugin.tar.gz` into the plugin directory in your Presto installation.

Next configure your spreadsheet catalog by adding a `zookeeper.properties` file in the `etc/catalog` directory in your Presto installation.

#### Example zookeeper.properties file:

    connector.name=zookeeper
    connection=localhost
    sessionTimeout=30000

###### Connect with Presto CLI

    java -jar presto-cli-*-executable.jar --catalog zookeeper

###### Show Schemas

    presto:default> show schemas;
            Schema         
    -----------------------
    information_schema
    zk
    (2 rows)

###### Show Tables

    presto:default> use zk;
    presto:zk> show tables;
               Table           
    ---------------------------
    zk 
    (1 row)

###### Select Data

    presto:zk> select path, data, version from zk where path like '/zookeeper';
     path       | data | version 
    ------------+------+---------
     /zookeeper |      |       0 

