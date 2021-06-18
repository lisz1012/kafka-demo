package com.lisz.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class Lesson02 {
	public static Properties init() {
		Properties conf = new Properties();
		conf.setProperty(ProducerConfig.ACKS_CONFIG, "0");
		conf.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		conf.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		conf.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "16384");
		conf.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
		conf.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");//16k，要调整，分析msg大小，尽量出发批次发送，减少内存碎片和系统调用的复杂度
		conf.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");
		conf.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
		conf.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
		conf.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
		conf.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		conf.setProperty(ProducerConfig.SEND_BUFFER_CONFIG, "32768");
		conf.setProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG, "32768");
		return conf;
	}

	public static void main(String[] args) {
		Properties conf = init();
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(conf);
		ProducerRecord<String, String> record = new ProducerRecord<>("ooxx", "hello", "hi");
		Future<RecordMetadata> future = producer.send(record);
		Future<RecordMetadata> send = producer.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {

			}
		});
	}
}