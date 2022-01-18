package com.lisz.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Lesson03 {
	/*
		kafka-topics.sh --zookeeper hadoop-02:2181/kafka --create --topic msb-items  --partitions 2 --replication-factor 2
		 */
	@Test
	public void producer() throws ExecutionException, InterruptedException {
		String topic = "items";
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		ProducerRecord<String, String> record
				= new ProducerRecord<>(topic, "item" + 1, "val" + 1); //
		Future<RecordMetadata> send = producer.send(record);
		RecordMetadata rm = send.get();
		int partition = rm.partition();
//		final List<PartitionInfo> partitionInfos = producer.partitionsFor("topic-1");
		long offset = rm.offset();
		System.out.println("key: " + record.key() + " val: " + record.value() + " partition: " + partition + " offset: " + offset + " timestamp: " + rm.timestamp());
	}

	@Test
	public void consumer() throws Exception {
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g1");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");    // 手动提交
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("items"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			if (!records.isEmpty()) {
				for (ConsumerRecord<String, String> record : records) {
					try {
						String str = record.key() + " - " + record.value();
//						int i = 2 / 0;      // 处理消息过程中出现异常！
						System.out.println(str);
						consumer.commitSync();     // 手动提交
					} catch (Exception e){
						Thread.sleep(10000);  // 试图解决和补偿，仍然劳而无功，但这次的设置是手动提交
					}
				}
			}
		}
	}
}
