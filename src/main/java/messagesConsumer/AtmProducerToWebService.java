/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package messagesConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;



/**
 *
 * @author fernando
 */
public class AtmProducerToWebService {

	private KafkaProducer<String, byte[]> producer;
	private Map<String,Map<String,String>> geoLoc = new HashMap<>();
	private String hbaseTable;
	private String hbaseGeoLocFamily;
	private String topic;
	
	public static final String USER_SCHEMA_WEBSERVICE = "{" + 
			"\"type\":\"record\"," + 
			"\"name\":\"atmRecord\"," + 
			"\"fields\":["	+ 
			"  { \"name\":\"id\", \"type\":\"string\" }," + 
			"  { \"name\":\"operValue\", \"type\":\"string\" }," + 
			"  { \"name\":\"lat\", \"type\":\"string\" }," +
			"  { \"name\":\"lng\", \"type\":\"string\" }" +
			"]}";


	static AtmProducerToWebService AtmProducerToWebServiceBuilder(
			String brokerServer, 
			String topic, 
			String hbaseTable, 
			String hbaseGeoLocColumnFamily
			) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerServer);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		AtmProducerToWebService atmProducer = new AtmProducerToWebService();
		atmProducer.producer = new KafkaProducer<>(props);
		atmProducer.hbaseGeoLocFamily = hbaseGeoLocColumnFamily;
		atmProducer.hbaseTable = hbaseTable;
		atmProducer.topic = topic;
		return atmProducer;
	}

	public void produce(String id, String value) {
		Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA_WEBSERVICE);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        
        Map<String,String> lat_lgn = geoLoc.get(id);        
        if(lat_lgn==null){
        	System.out.println("Going to Hbase to get lat and lng");
        	lat_lgn = SingletonVariablesShare.INSTANCE.getHbaseDAO().getValuesUnderFamiliys(hbaseTable, id,hbaseGeoLocFamily);
        }        
        GenericData.Record avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", id);
        avroRecord.put("operValue", value.toString());
        
        String lat = lat_lgn.get("lat")==null?"":lat_lgn.get("lat");
        String lng = lat_lgn.get("lng")==null?"":lat_lgn.get("lng");;
        
        avroRecord.put("lat", lat);
        avroRecord.put("lng", lng);        
        
        byte[] bytes = recordInjection.apply(avroRecord);
        
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, id, bytes);
		producer.send(record);
	}

	public void closeProducer() {
		producer.close();
	}

}
