Syslog -> Flume -> Kafka -> Storm -> Hive
=========================================
 
This is a simple demo project showing storm collecting syslog messages from a kafka topic, and pushing them into HDFS. It also rotates the files every n minutes into hive partitions.

Getting started
---------------

Use HDP 2.2.4, or adjust your version strings accordingly in the pom. 

On a kafka client run the setup script to create the syslog_events topic. 

Configure flume to push syslog events to this topic. 

Create a copy of the config.properties from resources, and point the hosts to right masters on your cluster.

Using hive, run the schema.sql file in the resources. Note that at present you will also need to chown the /apps/hive/warehouse/syslog_events folder to storm, assuming defaults, ultimately the storm topology should be running as a proper user, with access to this table. It's also worth considering the hive streaming bolt as an alternative to the hdfs bolt.

To submit the topology:

build with mvn package

/usr/hdp/current/storm-client/bin/storm jar target/syslog-0.0.1-SNAPSHOT.jar com.simonellistonball.demos.syslog.App /path/to/config.properties