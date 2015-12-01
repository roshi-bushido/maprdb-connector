package org.mule.modules.maprdb.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.components.Configuration;

@Configuration(friendlyName = "Configuration")
public class ConnectorConfig {
	private org.apache.hadoop.conf.Configuration hbaseConfig;

	@Configurable
	private String zookeeperQuorum;
	@Configurable
	private String clientPort;

	public synchronized org.apache.hadoop.conf.Configuration getConfiguration() {
		if (this.hbaseConfig == null) {
	    	this.hbaseConfig = HBaseConfiguration.create();
	    	this.hbaseConfig.set("hbase.zookeeper.quorum", this.zookeeperQuorum);
	    	this.hbaseConfig.set("hbase.zookeeper.property.clientPort",this.clientPort);
		}
    	return this.hbaseConfig;
	}

	public org.apache.hadoop.conf.Configuration getHbaseConfig() {
		return hbaseConfig;
	}

	public void setHbaseConfig(org.apache.hadoop.conf.Configuration hbaseConfig) {
		this.hbaseConfig = hbaseConfig;
	}

	public String getClientPort() {
		return clientPort;
	}

	public void setClientPort(String clientPort) {
		this.clientPort = clientPort;
	}

	public String getZookeeperQuorum() {
		return zookeeperQuorum;
	}

	public void setZookeeperQuorum(String zookeeperQuorum) {
		this.zookeeperQuorum = zookeeperQuorum;
	}
	
}