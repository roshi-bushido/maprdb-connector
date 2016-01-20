/**
 * Mule MapR Connector
 * 
 * Name: MaprDBConnector.java
 * Author: Matias Suarez (Mulesoft)
 * Version: 1.0.1
 * Description: Main Connector Class
 * 
 * Change History
 * Name		Date		Version		Modification
 * epesch	Nov 24 2015	1.0.1		Expanded "Select" capabilities, added documentation to all methods
 */

package org.mule.modules.maprdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.display.FriendlyName;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.display.Summary;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.annotations.param.Payload;
import org.mule.api.annotations.param.RefOnly;
import org.mule.modules.maprdb.config.ConnectorConfig;
import org.mule.modules.maprdb.model.Row;

/**
 * Mulesoft connector for connecting to a HBase Hadoop MapR Instance
 * 
 * @author epesch
 *
 */
@Connector(name="mapr-db", friendlyName="MaprDB", minMuleVersion = "3.6")
public class MaprDBConnector {
	private Log logger = LogFactory.getLog(MaprDBConnector.class);
	
	/**
	 * Connector Configuration object for connecting to MapR
	 */
    @Config
    ConnectorConfig config;

    /**
     * Get Connector Configuration
     * 
     * @return connector
     */
    public ConnectorConfig getConfig() {
        return config;
    }

    /**
     * Set Connector Configuration
     * 
     * @param config The connector configuration
     */
    public void setConfig(ConnectorConfig config) {
        this.config = config;
    }

    /**
     * Return a map of selected values from a given table
     * 
     * @param tableName 	Table name
     * @param filters		Reference to a org.apache.hadoop.conf.Filter object
     * @param columnLists	Reference to a Map<String, List<String>> object, where it's a Map of 'Column Family' to 'List of Column Qualifiers'
     * @param startKey		Key to start scan
     * @param limit			Number of rows to return
     * @param isReversed	Boolean to reverse result set returned
     * @return the result as a List of Rows, which contain a row key and a Map of 'Column Family' to a Map of 'Column Qualifier' and 'Value'
     * @throws IOException
     */
    @Processor(friendlyName="Select")
    public List<Row> select(
    		String tableName, 
    		@Optional @RefOnly @FriendlyName("Filter Reference") @Placement(group = "Query Parameters") @Summary("Reference to HBase Filter Object") Filter filters,
    		@Optional @RefOnly @FriendlyName("Column Reference") @Placement(group = "Query Parameters") @Summary("Reference to a Column List Object") Map<String, List<String>> columnLists,
    		@Optional @RefOnly @FriendlyName("Start Row Key") @Placement(group = "Query Parameters") @Summary("Start Row Key (useful for pagination)") String startKey,
    		@Default("0") @RefOnly @FriendlyName("Limit") @Placement(group = "Query Parameters") @Summary("Number of rows returned if available (set 0 for no limit)") int limit,
    		@Default("false") @FriendlyName("Reversed") @Placement(group = "Query Parameters") @Summary("Reverse scan (heavy operation)") boolean isReversed)
    				throws IOException {
    	//Setup Table
    	FilterList fullFilters = new FilterList();
    	HTable table = new HTable(this.config.getConfiguration(), tableName);
    	
    	//Setup scan
    	Scan scan = null;
    	//If reversed with startKey, set startKey as endKey with a zero byte (makes endKey inclusive rather than exclusive)
    	if(startKey != null && isReversed) {
			byte[] zeroByte = {0};
			scan = new Scan(new byte[]{0}, Bytes.add(startKey.getBytes(), zeroByte));
    	}
    	else if(startKey != null) scan = new Scan(startKey.getBytes());
    	else scan = new Scan();
    	
    	//Add filters
    	if(filters != null) fullFilters.addFilter(filters);
    	if(limit > 0 && !isReversed){
    		fullFilters.addFilter(new PageFilter(limit));
    	}
    	if(fullFilters.hasFilterRow()) scan.setFilter(fullFilters);
    	
    	//Add columns
    	if(columnLists != null) {
    		for(String columnFamily : columnLists.keySet()){
    			for(String columnQualifier : columnLists.get(columnFamily)){
    				scan.addColumn(columnFamily.getBytes(), columnQualifier.getBytes());
    			}
    		}
    	}
    	
    	//Process Scan
    	ResultScanner scanner = table.getScanner(scan);
    	List<Row> results = new ArrayList<Row>();
    	Result next = null;

    	//Iterate the scan results
    	while ((next = scanner.next()) != null) {
            Row currRow = new Row();
            Map<String, Map<String, String>> columnFamilies = new HashMap<String, Map<String, String>>();
            currRow.setKey(Bytes.toString(next.getRow()));
            
            //Iterate the column families
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyMap : next.getMap().entrySet()) {
            	Map<String, String> currFamily = new HashMap<String, String>();
            	String columnFamily = Bytes.toString(columnFamilyMap.getKey());
            	
            	//Iterate the columns
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entryVersion : columnFamilyMap.getValue().entrySet()) {
                    //Iterate the column values (if multiple versions)
                	for (Map.Entry<Long, byte[]> entry : entryVersion.getValue().entrySet()) {
                        String column = Bytes.toString(entryVersion.getKey());
                        String value = new String(entry.getValue());
                        currFamily.put(column, value);
                    }
                }
                
                columnFamilies.put(columnFamily, currFamily);
            }
            
            currRow.setColumnFamilyMap(columnFamilies);
            results.add(currRow);
        }
        table.close();
        
        //Handle isReversed operations (java only)
        if(isReversed){
        	Collections.reverse(results);
        	if(limit > 0 && limit < results.size()){
        		results = results.subList(0, limit);
        	}
        }
        
        return results;
    } 
    
    /**
     * Scan a given table
     * 
     * @param tableName Table name
     * @return the result as a List of Rows, which contain a row key and a Map of 'Column Family' to a Map of 'Column Qualifier' and 'Value'
     * @throws IOException
     */
    @Processor(friendlyName="Scan Table")
    public List<Row> scanTable(String tableName) throws IOException {
    	HTable table = new HTable(this.config.getConfiguration(), tableName);
    	Scan scan = new Scan();
    	ResultScanner scanner = table.getScanner(scan);

    	List<Row> results = new ArrayList<Row>();
    	Result next = null;

    	//Iterate the scan results
    	while ((next = scanner.next()) != null) {
            Row currRow = new Row();
            Map<String, Map<String, String>> columnFamilies = new HashMap<String, Map<String, String>>();
            currRow.setKey(Bytes.toString(next.getRow()));
            
            //Iterate the column families
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyMap : next.getMap().entrySet()) {
            	Map<String, String> currFamily = new HashMap<String, String>();
            	String columnFamily = Bytes.toString(columnFamilyMap.getKey());
            	
            	//Iterate the columns
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entryVersion : columnFamilyMap.getValue().entrySet()) {
                    //Iterate the column values (if multiple versions available)
                	for (Map.Entry<Long, byte[]> entry : entryVersion.getValue().entrySet()) {
                        String column = Bytes.toString(entryVersion.getKey());
                        String value = new String(entry.getValue());
                        currFamily.put(column, value);
                    }
                }
                
                columnFamilies.put(columnFamily, currFamily);
            }
            
            currRow.setColumnFamilyMap(columnFamilies);
            results.add(currRow);
        }
        
        table.close();
        return results;
    } 
    
    /**
     * Insert or update a row into a given table and column family
     * @param tableName Table Name
     * @param columnFamily Column Family
     * @param rows New or updated rows, in the form a Map of 'Row Key' to Map of 'Column Qualifier' to 'Value'
     * @throws IOException
     */
    @Processor
    public void upsert(String tableName, String columnFamily, @Payload Map<String, Map<String, String>> rows) throws IOException {
    	HTable table = new HTable(this.config.getConfiguration(), tableName);
    	this.logger.info(String.format("Writting %s rows to table %s", rows.keySet().size(), tableName));

    	final Set<Entry<String, Map<String, String>>> rowEntries = rows.entrySet();
    	for (Entry<String, Map<String, String>> entry : rowEntries) {
    	    String key = entry.getKey();
    	    Map<String, String> rowValues = entry.getValue();
    	    
    	    if (this.logger.isDebugEnabled()) {
    	    	this.logger.debug(String.format("RowId=%s, Values --> %", key, rowValues.toString()));
    	    }

    	    Put put = new Put(Bytes.toBytes(key));
    	    for (Map.Entry<String, String> rowEntry : rowValues.entrySet()) {
        	    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(rowEntry.getKey()), Bytes.toBytes(null2empty(rowEntry.getValue())));
    	    }
    	    table.put(put);
    	}
    	table.close();
    } 
    
    /**
     * Utility class for null string value conversion
     * 
     * @param o String
     * @return blank/empty if null, else the String value
     */
    private String null2empty(String o) {
    	if (o == null) return "";
		return o;
    }

}