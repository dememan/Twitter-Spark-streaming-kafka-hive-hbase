package cs523.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaService {

    public static final String BOOTSTRAPSERVERS  = "127.0.0.1:9092";
    public static final String TOPIC = "tweets";
    public static final String BOOTSTRAP_SERVERS_CONFIG = "StringDeserializer";
    public static final String  VALUE_DESERIALIZER_CLASS_CONFIG = "StringDeserializer";

    
	// Kafka Producer
	public static KafkaProducer<String, String> createKafkaProducer() {
		// Create producer properties
		Properties properties = new Properties();

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"localhost:9092");

		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);

		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		return new KafkaProducer<String, String>(properties);
	}

}
