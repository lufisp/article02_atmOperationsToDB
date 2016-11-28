package hbaseWriter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;



public class HbaseAdmin {

	private Configuration conf;
	private Connection connection;
	private Admin admin;

	public HbaseAdmin() {
		try {
			conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
		} catch (IOException e) {

		}
	}

	public void createNamespace(String _namespaceName) {
		try {
			TableName[] tbls = admin.listTableNamesByNamespace(_namespaceName);
			for (TableName tbl : tbls) {
				admin.disableTable(tbl);
				admin.deleteTable(tbl);
			}
			admin.deleteNamespace(_namespaceName);
			NamespaceDescriptor namespace = NamespaceDescriptor.create(_namespaceName).build();
			admin.createNamespace(namespace);
		} catch (IOException e) {
		}
		try {
			System.out.println("Creating namespace " + _namespaceName + "...");
			NamespaceDescriptor namespace = NamespaceDescriptor.create(_namespaceName).build();
			admin.createNamespace(namespace);
			System.out.println("Name Space: " + _namespaceName + " Created.");
		} catch (IOException e) {

		}

	}

	public void createTable(String _nameSpace, String _tableName,String... _columnFamilys) {		
		TableName tableName = TableName.valueOf(_nameSpace+":"+_tableName);
		HTableDescriptor desc = new HTableDescriptor(tableName);

		try {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block

		}

		for(String colFamily: _columnFamilys){
			HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes(colFamily));
			desc.addFamily(coldef);
		}

		System.out.println("Creating table " +_nameSpace+":"+_tableName +"...");

		try {
			admin.createTable(desc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Table: " + _nameSpace +":"+_tableName + " Created");

	}

	public static void main(String... args) {
		HbaseAdmin ah = new HbaseAdmin();
		ah.createNamespace("atm");
		ah.createTable("atm","AtmTotalCash","Total");		
	}

}