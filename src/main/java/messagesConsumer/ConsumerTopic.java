package messagesConsumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;


import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import hbaseAdo.HbaseDAO;


public class ConsumerTopic {
	
	/*Closing the constructor*/
	private ConsumerTopic(){}	
	
		
	private KafkaConsumer<String, byte[]> consumer;
	protected AtmProducerToWebService producer;
	protected String topic;
	protected Properties kafkaProps;
	protected HbaseDAO  hbaseDao;
	protected String hbaseTableName;
	protected String hbaseColumnFamilyName;
	protected String hbaseColumnName;
	
	public static final String USER_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"atmRecord\"," + "\"fields\":["
			+ "  { \"name\":\"id\", \"type\":\"string\" }," + "  { \"name\":\"operValue\", \"type\":\"int\" }" + "]}";

	public void startReading() {
		
		

		final Thread mainThread = Thread.currentThread();

		// Registering a shutdown hook so we can exit cleanly
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				System.out.println("Starting exit...");
				// Note that shutdownhook runs in a separate thread, so the only
				// thing we can safely do to a consumer is wake it up
				consumer.wakeup();
				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});

		try {
			consumer = new KafkaConsumer<>(kafkaProps);
			consumer.subscribe(Collections.singletonList(topic));

			Schema.Parser parser = new Schema.Parser();
			Schema schema = parser.parse(USER_SCHEMA);
			Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

			// looping until ctrl-c, the shutdown hook will cleanup on exit
			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(1000);
				System.out.println(System.currentTimeMillis() + "  --  waiting for data...");
				for (ConsumerRecord<String, byte[]> avroRecord : records) {
					//System.out.printf("offset = %d\n", avroRecord.offset());
					GenericRecord record = recordInjection.invert(avroRecord.value()).get();
					String id = record.get("id").toString();
					String value = record.get("operValue").toString();

					System.out.println("id= " + record.get("id") + ", operValue= " + record.get("operValue"));
					String valueHbaseString = hbaseDao.get(hbaseTableName,hbaseColumnFamilyName,hbaseColumnName, id);
					int valueHbase = valueHbaseString == "" ? 0 : Integer.parseInt(valueHbaseString);
					int valueUpdated = valueHbase + Integer.parseInt(value);
					producer.produce(id, String.valueOf(valueUpdated));
					hbaseDao.save(hbaseTableName,hbaseColumnFamilyName,hbaseColumnName, id, String.valueOf(valueUpdated));

				}
				//for (TopicPartition tp : consumer.assignment())
				//	System.out.println("Committing offset at position:" + consumer.position(tp));
				consumer.commitSync();
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			this.close();			
			System.out.println("Closed consumer and producer we are done");
		}
	}
	
	public void close(){
		this.consumer.close();
		this.producer.closeProducer();
		this.hbaseDao.closeConnection();
	}
	
	public static ConsumerTopic ConsumerTopicBuilder(String brokerServer, String topic, String groupId, AtmProducerToWebService producer){
		ConsumerTopic consumerTopic = new ConsumerTopic();
		consumerTopic.topic = topic;
		consumerTopic.kafkaProps = new Properties();
		consumerTopic.kafkaProps.put("group.id", groupId);
		consumerTopic.kafkaProps.put("bootstrap.servers", brokerServer);
		consumerTopic.kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerTopic.kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");		
		consumerTopic.producer = producer;
		consumerTopic.hbaseDao = SingletonVariablesShare.INSTANCE.getHbaseDAO();
		consumerTopic.hbaseColumnFamilyName = "Total";
		consumerTopic.hbaseColumnName = "cash";
		consumerTopic.hbaseTableName = "atm:AtmTotalCash";
		return consumerTopic;		
	}

}
