package com.simonellistonball.demos.syslog.schema;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SyslogSchema implements Scheme {
	private static final long serialVersionUID = -6739909360564801489L;
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat(
			"MMM d HH:mm:ss");
	private static final Pattern p = Pattern.compile(".[0-9]{1,2}.(.{15}) ([^ ]*)(.*)");

	private static final Logger LOG = Logger.getLogger(SyslogSchema.class);

	
	public List<Object> deserialize(byte[] bytes) {
		try {
			String event = new String(bytes, "UTF-8");
			Matcher m = p.matcher(event);
			if (m.matches()) {
				Date time = dateFormat.parse(m.group(1));
				String host = m.group(2);
				String message = m.group(3);
				return new Values(time, host, message);
			} else {
				LOG.warn("Broken message" + event);
				throw new RuntimeException("Broken message");
			}
		} catch (Exception e) {
			// Wrap parse errors in a runtime exception to prevent topology
			// dying on errors in data, in production you would handle this
			// better through logging, or routing a message to an exception
			// queue
			throw new RuntimeException(e);
		}
	}

	public Fields getOutputFields() {
		return new Fields("host", "timestamp", "message");
	}

}
