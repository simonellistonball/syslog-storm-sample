CREATE TABLE `syslog_events`(
  `timestamp` string,
  `host` string,
  `message` string) 
PARTITIONED BY (`date` string, `hour` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS textfile
 