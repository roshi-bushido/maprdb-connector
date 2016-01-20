/**
 * Mule MapR Connector
 * 
 * Name: ConnectorConfig.java
 * Author: Matias Suarez (Mulesoft)
 * Version: 1.0.1
 * Description: Connector Configuration profile for a MapR Client Connection
 * 
 * Change History
 * Name		Version		Modification
 * epesch	1.0.1		Added documentation
 */

package org.mule.modules.maprdb.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.components.Configuration;

/**
 * Values for connecting to a Hadoop Zookeper Quorum
 * 
 * @author epesch
 *
 */
@Configuration(friendlyName = "Configuration")
public class ConnectorConfig {
	/**
	 * The Hadoop Configuration object
	 */
	private org.apache.hadoop.conf.Configuration hbaseConfig;

	/**
	 * The quorum list for MapR
	 */
	@Configurable
	private String zookeeperQuorum;
	
	/**
	 * The port for client connections in the quorum
	 */
	@Configurable
	private String clientPort;
	
	/**
	 * Constructor for Hadoop Configuration object
	 * 
	 * @return constructed Hadoop Configuration object
	 */
	public synchronized org.apache.hadoop.conf.Configuration getConfiguration() {
		if (this.hbaseConfig == null) {
	    	this.hbaseConfig = HBaseConfiguration.create();
	    	this.hbaseConfig.set("hbase.zookeeper.quorum", this.zookeeperQuorum);
	    	this.hbaseConfig.set("hbase.zookeeper.property.clientPort",this.clientPort);
		}
		
    	return this.hbaseConfig;
	}

	/**
	 * Get Hadoop Configuration object
	 * 
	 * @return Hadoop Configuration object
	 */
	public org.apache.hadoop.conf.Configuration getHbaseConfig() {
		return hbaseConfig;
	}

	/**
	 * Set Hadoop Configuration object
	 * 
	 * @param hbaseConfig The Configuration object
	 */
	public void setHbaseConfig(org.apache.hadoop.conf.Configuration hbaseConfig) {
		this.hbaseConfig = hbaseConfig;
	}

	/**
	 * Get Configuration client port
	 * 
	 * @return client port
	 */
	public String getClientPort() {
		return clientPort;
	}

	/**
	 * Set Configuration client port
	 * 
	 * @param clientPort The client port
	 */
	public void setClientPort(String clientPort) {
		this.clientPort = clientPort;
	}

	/**
	 * Get Configuration zookeeper quorum
	 * 
	 * @return zookeeper quorum
	 */
	public String getZookeeperQuorum() {
		return zookeeperQuorum;
	}

	/**
	 * Set Configuration zookeeper quorum
	 * 
	 * @param zookeeperQuorum The zookeeper quorum
	 */
	public void setZookeeperQuorum(String zookeeperQuorum) {
		this.zookeeperQuorum = zookeeperQuorum;
	}
	
}