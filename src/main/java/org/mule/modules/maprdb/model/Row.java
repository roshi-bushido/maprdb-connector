/**
 * Mule MapR Connector
 * 
 * Name: Row.java
 * Author: Eric Pesch (epesch)
 * Version: 1.0.0
 * Description: DTO for a MapR row
 * 
 * Change History
 * Name		Version		Modification
 */

package org.mule.modules.maprdb.model;

import java.util.HashMap;
import java.util.Map;

public class Row {
	/**
	 * Row Key
	 */
	String key;
	
	/**
	 * Map of a specific Column Family to its Columns and Values
	 */
	Map<String, Map<String, String>> columnFamilyMap;
	
	/**
	 * Constructor
	 */
	public Row() { }
	
	/**
	 * Retrieve a row's key
	 * 
	 * @return row key
	 */
	public String getKey() {
		return key;
	}
	
	/**
	 * Set a row's key
	 * 
	 * @param key row key
	 */
	public void setKey(String key) {
		this.key = key;
	}
	
	/**
	 * Retrieve a Column Family Map by Column Family
	 * 
	 * @param columnFamily Column Family name
	 * @return map of column family's columns and values
	 */
	public Map<String, String> getColumnFamily(String columnFamily){
		if(columnFamilyMap != null) return columnFamilyMap.get(columnFamily);
		return null;
	}
	
	/**
	 * Add a new Column Family to a row
	 * 
	 * @param columnFamily Column Family name
	 * @param columns Map of Column Qualifier and values
	 * @return old value for the Column Family name, or null if no previous value
	 */
	public Map<String, String> addColumnFamily(String columnFamily, Map<String, String> columns){
		if(columnFamilyMap != null) columnFamilyMap = new HashMap<String, Map<String, String>>();
		return columnFamilyMap.put(columnFamily, columns);
	}
	
	/**
	 * Remove a Column Family from the row
	 * 
	 * @param columnFamily Column Family name
	 * @return old value for the Column Family name, or null if no value found
	 */
	public Map<String, String> removeColumnFamily(Map<String, String> columnFamily){
		if(columnFamilyMap != null) return columnFamilyMap.remove(columnFamily);
		return null;
	}
	
	/**
	 * Retrieve full Column Family Map
	 * 
	 * @return Map of Column Family to Column Family's Columns and Values
	 */
	public Map<String, Map<String, String>> getColumnFamilyMap() {
		return columnFamilyMap;
	}
	
	/**
	 * Set full Column Family Map
	 * 
	 * @param columnFamilyMap Map of Column Family to Column Family's Columns and Values
	 */
	public void setColumnFamilyMap(
			Map<String, Map<String, String>> columnFamilyMap) {
		this.columnFamilyMap = columnFamilyMap;
	}
}
