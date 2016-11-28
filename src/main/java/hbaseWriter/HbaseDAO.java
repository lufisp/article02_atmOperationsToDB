package hbaseWriter;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;



public class HbaseDAO {

	protected Configuration conf;
	protected Connection connection;
	protected Admin admin;

	protected String tableName;
	protected String columnFamily;
	protected String column;
	
	protected boolean _DEBUG = true;

	public HbaseDAO() {
		try {
			conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
			tableName = "ypr:patients";
			columnFamily = "Info";
			column = "json";
		} catch (IOException e) {

		}
	}

	
	public void save(String _tableName,String _columnFamily, String _columnName, int id, int _value) {
		if(_DEBUG) System.out.println("Saving value...");
		try {
			Table table = connection.getTable(TableName.valueOf(_tableName));
			Put put = new Put(Bytes.toBytes(id));			
			put.addColumn(Bytes.toBytes(_columnFamily), Bytes.toBytes(_columnName), Bytes.toBytes(_value));
			table.put(put);
			table.close();
			// connection.close();
		} catch (IOException e) {
			e.printStackTrace();			
		}
		if(_DEBUG) System.out.println("Done...");
	}
	
	
	public void delete(String _tableName, int id) {
		if(_DEBUG) System.out.println("Deleting row...");
		try {
			Table table = connection.getTable(TableName.valueOf(_tableName));
			Get get = new Get(Bytes.toBytes(id));
			Result result = table.get(get);
			if (!result.isEmpty()) {
				Delete delete = new Delete(Bytes.toBytes(id));
				table.delete(delete);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(_DEBUG) System.out.println("Done...");

	}

	public int get(String _tableName,String _columnFamily, String _columnName,  int id) {
		Integer value = 0;
		try {
			Table table = connection.getTable(TableName.valueOf(_tableName));
			Get get = new Get(Bytes.toBytes(id));
			get.addColumn(Bytes.toBytes(_columnFamily), Bytes.toBytes(_columnName));
			Result result = table.get(get);
			if (result.isEmpty()) {
				this.save(_tableName, _columnFamily, _columnName, id, value);				
			} else {
				value =  Bytes.toInt(result.getValue(Bytes.toBytes(_columnFamily), Bytes.toBytes(_columnName)));				
			}
		} catch (IOException e) {
			e.printStackTrace();			
		}
		return value;
	}	

	
	public static final void main(String... args){
		HbaseDAO hbDAO = new HbaseDAO();
		//hbDAO.save("atm:AtmTotalCash", "Total", "cash", 2, 176);
		hbDAO.delete("atm:AtmTotalCash", 1);
		hbDAO.delete("atm:AtmTotalCash", 2);
		//System.out.println("Value: " +  hbDAO.get("atm:AtmTotalCash", "Total", "cash", 2));
	}

}