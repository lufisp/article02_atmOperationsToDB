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
import org.springframework.stereotype.Component;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

@Component
public class ConsumerTopic {
	
	private Properties kafkaProps = new Properties();
	private KafkaConsumer<String, byte[]> consumer;
	// private topic = "toHbase";
	private static String topic = "atmOperations";

	public static final String USER_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"atmRecord\"," + "\"fields\":["
			+ "  { \"name\":\"id\", \"type\":\"string\" }," + "  { \"name\":\"operValue\", \"type\":\"int\" }" + "]}";

	public void startReading() {

		final ConsumerTopic toHbaseConsumer = new ConsumerTopic();
		toHbaseConsumer.configure("localhost:9092");

		final Thread mainThread = Thread.currentThread();

		// Registering a shutdown hook so we can exit cleanly
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				System.out.println("Starting exit...");
				// Note that shutdownhook runs in a separate thread, so the only
				// thing we can safely do to a consumer is wake it up
				toHbaseConsumer.consumer.wakeup();
				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});

		try {
			toHbaseConsumer.consumer.subscribe(Collections.singletonList(topic));

			Schema.Parser parser = new Schema.Parser();
			Schema schema = parser.parse(USER_SCHEMA);
			Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

			// looping until ctrl-c, the shutdown hook will cleanup on exit
			while (true) {
				ConsumerRecords<String, byte[]> records = toHbaseConsumer.consumer.poll(1000);
				System.out.println(System.currentTimeMillis() + "  --  waiting for data...");
				for (ConsumerRecord<String, byte[]> avroRecord : records) {
					System.out.printf("offset = %d\n", avroRecord.offset());
					GenericRecord record = recordInjection.invert(avroRecord.value()).get();

					System.out.println("id= " + record.get("id") + ", operValue= " + record.get("operValue"));

				}
				for (TopicPartition tp : toHbaseConsumer.consumer.assignment())
					System.out.println("Committing offset at position:" + toHbaseConsumer.consumer.position(tp));
				toHbaseConsumer.consumer.commitSync();
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			toHbaseConsumer.consumer.close();
			System.out.println("Closed consumer and we are done");
		}
	}

	private void configure(String servers) {
		kafkaProps.put("group.id", "gp1");
		kafkaProps.put("bootstrap.servers", servers);
		kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		consumer = new KafkaConsumer<String, byte[]>(kafkaProps);
	}

}
