package messagesConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hbaseAdo.HbaseDAO;

public enum SingletonVariablesShare {
	INSTANCE;
	private Map<String, Map<String,String>> atmGeoLoc = new HashMap<>();
	private HbaseDAO hbaseDAO = new HbaseDAO();

	public Map<String, Map<String, String>> getAtmGeoLoc() {
		return atmGeoLoc;
	}

	public void setAtmGeoLoc(Map<String, Map<String, String>> atmGeoLoc) {
		this.atmGeoLoc = atmGeoLoc;
	}

	public HbaseDAO getHbaseDAO() {
		return hbaseDAO;
	}

	public void setHbaseDAO(HbaseDAO hbaseDAO) {
		this.hbaseDAO = hbaseDAO;
	}
	

		
	
}
