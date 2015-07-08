package com.simonellistonball.demos.syslog.topologies;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.hortonworks.streaming.impl.bolts.hdfs.FileTimeRotationPolicy;
import com.hortonworks.streaming.impl.bolts.hive.HiveTablePartitionAction;
import com.simonellistonball.demos.syslog.schema.SyslogSchema;

public class BasicSyslogTopology {

	private static final Logger LOG = Logger.getLogger(BasicSyslogTopology.class);

	protected Properties topologyConfig;

	public BasicSyslogTopology(String configFileLocation) throws Exception {
		topologyConfig = new Properties();
		try {
			topologyConfig.load(new FileInputStream(configFileLocation));
		} catch (FileNotFoundException e) {
			LOG.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		} catch (IOException e) {
			LOG.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		}
	}

	/**
	 * Creates the spouts and bolts, then wires them up. This will then submit
	 * the topology to the storm nimbus server to run on the cluster
	 * 
	 * @throws Exception
	 */
	public void buildAndSubmit() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		// Set up Kafka spout
		configureKafkaSpout(builder);

		// Set up HDFSBolt
		configureHDFSBolt(builder);

		/*
		 * This conf is for Storm and it needs be configured with things like
		 * the following: Zookeeper server, nimbus server, ports, etc... All of
		 * this configuration will be picked up in the ~/.storm/storm.yaml file
		 * that will be located on each storm node, or your local dev
		 * environment
		 */
		Config conf = new Config();
		conf.setDebug(true);
		/*
		 * Set the number of workers that will be spun up for this topology.
		 * Each worker represents a JVM where executor thread will be spawned
		 * from
		 */
		Integer topologyWorkers = Integer.valueOf(topologyConfig
				.getProperty("storm.syslog.topology.workers"));
		conf.put(Config.TOPOLOGY_WORKERS, topologyWorkers);

		// Read the nimbus host in from the config file as well
		String nimbusHost = topologyConfig.getProperty("nimbus.host");
		conf.put(Config.NIMBUS_HOST, nimbusHost);

		try {
			StormSubmitter.submitTopology("syslog-event-processor", conf,
					builder.createTopology());
		} catch (Exception e) {
			LOG.error("Error submiting Topology", e);
		}

	}

	/**
	 * Construct and configure the HDFS bolt
	 * 
	 * This also interacts with Hive to provide a table on top of the emitted
	 * HDFS files, and manage adding partitions to the table as the file is
	 * rotated.
	 * 
	 * @param builder
	 */
	private void configureHDFSBolt(TopologyBuilder builder) {
		// Use pipe as record boundary

		String rootPath = topologyConfig.getProperty("hdfs.path");
		String prefix = topologyConfig.getProperty("hdfs.file.prefix");
		String fsUrl = topologyConfig.getProperty("hdfs.url");
		Float rotationTimeInMinutes = Float.valueOf(topologyConfig
				.getProperty("hdfs.file.rotation.time.minutes"));

		// some settings to interact with the hive metastore so the bolt can
		// inform hive of the new data partitions represented by the files
		// written to HDFS.
		String sourceMetastoreUrl = topologyConfig
				.getProperty("hive.metastore.url");
		String hiveStagingTableName = topologyConfig
				.getProperty("hive.staging.table.name");
		String databaseName = topologyConfig.getProperty("hive.database.name");

		// The HDFS bolt supports CSV files, and sequence files at the moment.
		// To write out ORC files the hive streaming bolt should be used.
		RecordFormat format = new DelimitedRecordFormat()
				.withFieldDelimiter(",");

		// Synchronize data buffer with the filesystem every 1000 tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);

		// Rotate data files when they reach five MB
		// FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

		// Rotate every X minutes
		FileTimeRotationPolicy rotationPolicy = new FileTimeRotationPolicy(
				rotationTimeInMinutes, FileTimeRotationPolicy.Units.MINUTES);

		// Hive Partition Action - this is included to add the file to a hive
		// table
		HiveTablePartitionAction hivePartitionAction = new HiveTablePartitionAction(
				sourceMetastoreUrl, hiveStagingTableName, databaseName, fsUrl);

		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(
				rootPath + "/staging").withPrefix(prefix);

		// Instantiate the HdfsBolt
		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(fsUrl)
				.withFileNameFormat(fileNameFormat).withRecordFormat(format)
				.withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy)
				.addRotationAction(hivePartitionAction);

		int hdfsBoltCount = Integer.valueOf(topologyConfig
				.getProperty("hdfsbolt.thread.count"));
		builder.setBolt("hdfs_bolt", hdfsBolt, hdfsBoltCount).shuffleGrouping(
				"kafkaSpout");
	};

	private void configureKafkaSpout(TopologyBuilder builder) {
		KafkaSpout kafkaSpout = constructKafkaSpout();

		int spoutCount = Integer.valueOf(topologyConfig
				.getProperty("spout.thread.count"));
		
		builder.setSpout("kafkaSpout", kafkaSpout, spoutCount);
	}

	/**
	 * Construct the KafkaSpout
	 * 
	 * @return
	 */
	private KafkaSpout constructKafkaSpout() {
		KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
		return kafkaSpout;
	}

	/**
	 * Construct the configuration object for the kafka spout.
	 * 
	 * @return
	 */
	private SpoutConfig constructKafkaSpoutConf() {
		BrokerHosts hosts = new ZkHosts(
				topologyConfig.getProperty("kafka.zookeeper.host.port"));
		String topic = topologyConfig.getProperty("kafka.topic");
		String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
		String consumerGroupId = topologyConfig
				.getProperty("kafka.consumer.group.id");

		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot,
				consumerGroupId);

		/*
		 * Custom Schema that will take Kafka message of single syslog event and
		 * emit a tuple containing the headers, and body of the event
		 */
		spoutConfig.scheme = new SchemeAsMultiScheme(new SyslogSchema());

		return spoutConfig;
	}
}