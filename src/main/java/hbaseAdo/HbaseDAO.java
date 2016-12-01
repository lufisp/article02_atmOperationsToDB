package hbaseAdo;


import java.io.IOException;

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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;



public class HbaseDAO {

	protected Configuration conf;
	protected Connection connection;
	protected Admin admin;	
	protected boolean _DEBUG = false;

	public HbaseDAO() {
		try {
			conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();			
		} catch (IOException e) {

		}
	}
	public void closeConnection(){
		try {
			this.connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	public void save(String _tableName,String _columnFamily, String _columnName, String id, int _value) {
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
	
	
	public void delete(String _tableName, String id) {
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

	public int get(String _tableName,String _columnFamily, String _columnName, String id) {
		Integer value = 0;
		try {
			Table table = connection.getTable(TableName.valueOf(_tableName));
			Get get = new Get(Bytes.toBytes(id));
			get.addColumn(Bytes.toBytes(_columnFamily), Bytes.toBytes(_columnName));
			Result result = table.get(get);
			if (!result.isEmpty()) {
				value =  Bytes.toInt(result.getValue(Bytes.toBytes(_columnFamily), Bytes.toBytes(_columnName)));				
			}
		} catch (IOException e) {
			e.printStackTrace();			
		}
		return value;
	}	

	
	public static final void main(String... args){
		HbaseDAO hbDAO = new HbaseDAO();
		hbDAO.save("atm:AtmTotalCash", "Total", "cash", "1", 1070);
		hbDAO.save("atm:AtmTotalCash", "Total", "cash", "2", 1770);
		hbDAO.save("atm:AtmTotalCash", "Total", "cash", "3", 10000);
		hbDAO.save("atm:AtmTotalCash", "Total", "cash", "4", 1800);
		hbDAO.save("atm:AtmTotalCash", "Total", "cash", "5", 1960);
		hbDAO.save("atm:AtmTotalCash", "Total", "cash", "6", 17600);
		hbDAO.save("atm:AtmTotalCash", "Total", "cash", "7", 1070);
		hbDAO.save("atm:AtmTotalCash", "Total", "cash", "8", 2760);
		hbDAO.save("atm:AtmTotalCash", "Total", "cash", "9", 3760);
		//hbDAO.delete("atm:AtmTotalCash", 1);
		//hbDAO.delete("atm:AtmTotalCash", 2);
		//System.out.println("Value: " +  hbDAO.get("atm:AtmTotalCash", "Total", "cash", 2));
	}

}