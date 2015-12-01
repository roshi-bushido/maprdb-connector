package com.mulesoft.mapr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MainTest extends TestCase {

	@Test
	public void shouldScanATable() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "/tables/cases");
        Scan s = new Scan();
        ResultScanner scanner = table.getScanner(s);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext())
        {
            Result next = iterator.next();
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyMap : next.getMap().entrySet())
            {
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entryVersion : columnFamilyMap.getValue().entrySet())
                {
                    for (Map.Entry<Long, byte[]> entry : entryVersion.getValue().entrySet())
                    {
                        String row = Bytes.toString(next.getRow());
                        String column = Bytes.toString(entryVersion.getKey());
                        String value = new String(entry.getValue());
                        System.out.println(String.format("row: %s, column: %s, value: %s", row, column, value));
                    }
                }
            }
        }
        table.close();		
	}


	@Test
	public void shouldWriteToATable() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "/tables/projects");
        String columnFamily = "info";
        
        Map<String, String> row1 = new HashMap<>();
        row1.put("name", "emmanuel");
        row1.put("lastname", "teodoro");
        
        Map<String, String> row2 = new HashMap<>();
        row2.put("name", "jorge");
        row2.put("lastname", "garci2a");

        Map<String, Map<String, String>> rows = new HashMap<>();
		rows.put("row1", row1);
		rows.put("row2", row2);
        
    	final Set<Entry<String, Map<String, String>>> rowEntries = rows.entrySet();
    	for (Entry<String, Map<String, String>> entry : rowEntries) {
    	    String key = entry.getKey();
    	    Map<String, String> rowValues = entry.getValue();
    	    Put put = new Put(Bytes.toBytes(key));
    	    for (Map.Entry<String, String> rowEntry : rowValues.entrySet()) {
        	    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(rowEntry.getKey()), Bytes.toBytes(rowEntry.getValue()));
    	    }
    	    table.put(put);
    	}
    	table.close();        
	}

}
