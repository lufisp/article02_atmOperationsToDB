package hbaseAdo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

	public void closeConnection() {
		try {
			this.connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void save(String _tableName, String _columnFamily, String _columnName, String id, String _value) {
		if (_DEBUG)
			System.out.println("Saving value...");
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
		if (_DEBUG)
			System.out.println("Done...");
	}

	public void delete(String _tableName, String id) {
		if (_DEBUG)
			System.out.println("Deleting row...");
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
		if (_DEBUG)
			System.out.println("Done...");

	}

	public String get(String _tableName, String _columnFamily, String _columnName, String id) {
		String value = "";
		try {
			Table table = connection.getTable(TableName.valueOf(_tableName));
			Get get = new Get(Bytes.toBytes(id));
			get.addColumn(Bytes.toBytes(_columnFamily), Bytes.toBytes(_columnName));
			Result result = table.get(get);
			if (!result.isEmpty()) {
				value = Bytes.toString(result.getValue(Bytes.toBytes(_columnFamily), Bytes.toBytes(_columnName)));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return value;
	}

	/**
	 * 
	 * @param _tableName
	 * @param id
	 * @param _columnFamily
	 * @return HashMap
	 * 
	 *         This methode receive a tablename, an id, and one array of
	 *         columnfamily and returns all the columns with the values under
	 *         the columnsfamilies
	 * 
	 */
	public Map<String, String> getValuesUnderFamiliys(String _tableName, String id, String... _columnFamily) {
		HashMap<String, String> resultMap = new HashMap<>();
		try {
			Table table = connection.getTable(TableName.valueOf(_tableName));
			Get get = new Get(Bytes.toBytes(id));
			for (String columnFam : _columnFamily)
				get.addFamily(Bytes.toBytes(columnFam));
			Result result = table.get(get);
			resultMap.put("key", id);
			for (byte[] columnFamily : result.getMap().keySet()) {
				for (byte[] column : result.getFamilyMap(columnFamily).keySet()) {
					resultMap.put(Bytes.toString(column), Bytes.toString(result.getValue(columnFamily, column)));
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultMap;
	}

	public static final void main(String... args) {
		HbaseDAO hbDAO = new HbaseDAO();
		
		/*
		System.out.println(hbDAO.getValuesUnderFamiliys("atm:AtmTotalCash", "1", "GeoLoc"));
		System.out.println(hbDAO.getValuesUnderFamiliys("atm:AtmTotalCash", "2", "Total", "GeoLoc"));
		System.out.println(hbDAO.getValuesUnderFamiliys("atm:AtmTotalCash", "3", "Total", "GeoLoc"));
        */
		
		  hbDAO.save("atm:AtmTotalCash", "Total", "cash", "1", "100000");
		  hbDAO.save("atm:AtmTotalCash", "Total", "cash", "2", "100000");
		  hbDAO.save("atm:AtmTotalCash", "Total", "cash", "3", "100000");
		  hbDAO.save("atm:AtmTotalCash", "Total", "cash", "4", "100000");
		  hbDAO.save("atm:AtmTotalCash", "Total", "cash", "5", "100000");
		  hbDAO.save("atm:AtmTotalCash", "Total", "cash", "6", "100000");
		  hbDAO.save("atm:AtmTotalCash", "Total", "cash", "7", "100000");
		  hbDAO.save("atm:AtmTotalCash", "Total", "cash", "8", "100000");
		  hbDAO.save("atm:AtmTotalCash", "Total", "cash", "9", "100000");
		 

		/*
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lat", "1", "45.494676");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lng", "1", "-73.562307");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lat", "2", "45.504813");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lng", "2", "-73.577156");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lat", "3", "45.495203");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lng", "3", "-73.578851");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lat", "4", "45.526374");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lng", "4", "-73.569345");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lat", "5", "45.534116");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lng", "5", "-73.564314");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lat", "6", "45.503219");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lng", "6", "-73.618183");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lat", "7", "45.516361");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lng", "7", "-73.577671");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lat", "8", "45.489599");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lng", "8", "-73.566572");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lat", "9", "45.509475");
		 * hbDAO.save("atm:AtmTotalCash", "GeoLoc", "lng", "9", "-73.553467");
		 */
		// hbDAO.delete("atm:AtmTotalCash", 1);
		// hbDAO.delete("atm:AtmTotalCash", 2);
		// System.out.println("Value: " + hbDAO.get("atm:AtmTotalCash", "Total",
		// "cash", 2));
	}

}