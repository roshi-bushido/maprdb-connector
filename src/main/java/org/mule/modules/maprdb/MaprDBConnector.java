package org.mule.modules.maprdb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.param.Payload;
import org.mule.modules.maprdb.config.ConnectorConfig;


@Connector(name="mapr-db", friendlyName="MaprDB")
public class MaprDBConnector {
	private Log logger = LogFactory.getLog(MaprDBConnector.class);
	
    @Config
    ConnectorConfig config;

    public ConnectorConfig getConfig() {
        return config;
    }

    public void setConfig(ConnectorConfig config) {
        this.config = config;
    }
    
    @Processor
    public Map<String, Map<String, String>> select(String tableName) throws IOException {
    	HTable table = new HTable(this.config.getConfiguration(), tableName);
    	Scan scan = new Scan();
    	ResultScanner scanner = table.getScanner(scan);

    	Map<String, Map<String, String>> results = new HashMap<String, Map<String, String>>();

    	Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext())
        {
            Result next = iterator.next();
            String row = Bytes.toString(next.getRow());
            results.put(row, new HashMap<String,String>()); // initialize the map
            
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyMap : next.getMap().entrySet())
            {
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entryVersion : columnFamilyMap.getValue().entrySet())
                {
                    for (Map.Entry<Long, byte[]> entry : entryVersion.getValue().entrySet())
                    {
                        String column = Bytes.toString(entryVersion.getKey());
                        String value = new String(entry.getValue());
                        results.get(row).put(column, value);
                    }
                }
            }
        }
        table.close();
        return results;
    } 
    
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
    
    private String null2empty(String o) {
    	if (o == null) {
    		return "";
    	} else {
    		return o;
    	}
    }

}