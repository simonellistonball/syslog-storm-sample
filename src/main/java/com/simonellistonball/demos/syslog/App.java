package com.simonellistonball.demos.syslog;

import com.simonellistonball.demos.syslog.topologies.BasicSyslogTopology;

/**
 * Main entry point to create and submit topology
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		String configFileLocation = args[0];
		BasicSyslogTopology topology = new BasicSyslogTopology(configFileLocation);
		topology.buildAndSubmit();
	}
}
