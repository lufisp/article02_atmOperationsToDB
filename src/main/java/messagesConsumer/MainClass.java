package messagesConsumer;

public class MainClass {
	
	
	ConsumerTopic toHbaseConsumer;
	AtmProducerToWebService producer;
	
	/*Utilization example
	mvn clean package	
	java -jar target/uber-article02_toHbaseConsumer-0.0.1-SNAPSHOT.jar localhost:9092 atmOperations gp01 localhost:9092 atmOperationsToWebService  
    */
	public static void main(String[] args) {
		System.out.println("Start reading kafka queue...");
		MainClass mainClass = new MainClass();
		
		String brokerListConsumer = args[0]; //"localhost:9092";
		String topic = args[1]; //"atmOperations";
		String groupId = args[2]; //"gp01";
		String brokerListProducer = args[3];
		String topicProducer = args[4];
	
		mainClass.producer = AtmProducerToWebService.AtmProducerToWebServiceBuilder(brokerListProducer, topicProducer);
		
		mainClass.toHbaseConsumer = ConsumerTopic.ConsumerTopicBuilder(
				brokerListConsumer, 
				topic, 
				groupId,
				mainClass.producer);
		mainClass.toHbaseConsumer.startReading();		

	}

}
