package messagesConsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
public class MainClass {
	
	@Autowired
	ConsumerTopic toHbaseConsumer;
	
	public void start(){
		toHbaseConsumer = new ConsumerTopic();
		toHbaseConsumer.startReading();
	}
	
	public static void main(String[] args) {
		ApplicationContext context = new AnnotationConfigApplicationContext(MainClass.class);
		System.out.println("Start reading kafka queue...");
		new MainClass().start();		

	}

}
