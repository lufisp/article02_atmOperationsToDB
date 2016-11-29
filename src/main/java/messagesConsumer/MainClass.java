package messagesConsumer;

public class MainClass {
	
	
	ConsumerTopic toHbaseConsumer;
	
	/*Utilization example
	mvn clean package	
	java -jar target/uber-article02_toHbaseConsumer-0.0.1-SNAPSHOT.jar localhost:9092 atmOperations gp01
    */
	public static void main(String[] args) {
		System.out.println("Start reading kafka queue...");
		MainClass mainClass = new MainClass();
		
		String brokerList = args[0]; //"localhost:9092";
		String topic = args[1]; //"atmOperations";
		String groupId = args[2]; //"gp01";
		
		
		mainClass.toHbaseConsumer = ConsumerTopic.ConsumerTopicBuilder(brokerList, topic, groupId);
		mainClass.toHbaseConsumer.startReading();		

	}

}
