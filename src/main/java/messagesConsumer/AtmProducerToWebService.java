/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package messagesConsumer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author fernando
 */
public class AtmProducerToWebService {

	private KafkaProducer<String, String> producer;

	static AtmProducerToWebService AtmProducerToWebServiceBuilder(String brokerServer, String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		AtmProducerToWebService atmProducer = new AtmProducerToWebService();
		atmProducer.producer = new KafkaProducer<>(props);
		return atmProducer;
	}

	public void produce(String id, String value) {
		ProducerRecord<String, String> record = new ProducerRecord<>("atmOperationsToWebService", id, value);
		producer.send(record);
	}

	public void closeProducer() {
		producer.close();
	}

}
