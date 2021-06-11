package com.lisz.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
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
				StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g3");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// 自动提交容易造成丢数据和重复消费数据. 一个运行的consumer程序，自己会维护消费进度
		// poll的时候都能够poll对，不会重复的。一旦自动提交但是是异步的，有可能：1.挂的时候还没提交，
		// 则重复消费上次提交到挂之前消费的。 2。poll出来的这一批还没消费完，就异步提交了，然后挂了，
		// 则消费位置到提交位置之间的消息就丢失了
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "15000");
//		// 拉取数据：弹性的、按需的，拉取多少？
//		props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("msb-items"));
		while (true) {
			// 0-n 条, 微批的感觉
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
			if (!records.isEmpty()) {
				System.out.println("--------------" + records.count() + "--------------");
				Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
				while (iterator.hasNext()) {
					// 因为一个consumer可以消费多个分区，但是一个分区只能给一个组里的一个consumer消费
					ConsumerRecord<String, String> record = iterator.next();
					int partition = record.partition();
					long offset = record.offset();
					System.out.println("key: " + record.key() + " val: " + record.value()
							+ " partition: " + partition + " offset: " + offset);
				}
			}
		}
	}

}
