import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Created by $maoshuwu on 2020/11/6.
 */
public class ProducerTest {
    private static final String[] WORDS = {
            "hello", "hadoop", "java", "kafka", "spark"
    };

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.21:9092,192.168.5.22:9092,192.168.5.23:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(props);
        boolean flag = true;
        while (flag) {
            for (int i = 0; i < 500; i++) {
                //3、发送数据
                kafkaProducer.send(new ProducerRecord("mswtest", WORDS[new Random().nextInt(5)]));
            }
            kafkaProducer.flush();
            System.out.println("==========Kafka Flush==========");
            Thread.sleep(5000);
        }

        kafkaProducer.close();
    }

}
