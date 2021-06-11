package com.lisz.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Lesson01 {
	/*
	kafka-topics.sh --zookeeper hadoop-02:2181/kafka --create --topic msb-items  --partitions 2 --replication-factor 2
	 */
	@Test
	public void producer() throws ExecutionException, InterruptedException {
		String topic = "msb-items";
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"hadoop-02:9092,hadoop-03:9092,hadoop-04:9092");
		// Kafka是一个能够持久化数据的MQ，数据是以byte[], 不会对数据进行加工，所以双方要约定编解码
		// kafka是一个app，可以使用0拷贝，sendfile实现快速数据消费
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		//producer就是一个提供者，面向的其实是Broker，虽然在使用的时候，我们期望的是把数据打入topic
		/*
		msb-items
		三种商品，每种商品有线性的3个ID，相同的key最好去到同一个分区
		 */

		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 3; j++) {
				ProducerRecord<String, String> record
						= new ProducerRecord<>(topic,"item" + j, "val" + i);
				Future<RecordMetadata> send = producer.send(record);
				RecordMetadata rm = send.get();
				int partition = rm.partition();
				long offset = rm.offset();
				System.out.println("key: " + record.key() + " val: " + record.value() +
								  " partition: " + partition + " offset: " + offset);
			}
		}
	}


	@Test
	public void consumer() {
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"hadoop-02:9092,hadoop-03:9092,hadoop-04:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g1");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	}

}
