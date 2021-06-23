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
		conf.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); //16k，要调整，分析msg大小，尽量触发批次发送，减少内存碎片和系统调用的复杂度。因为batch装不下一条大的记录的时候，就会高出一个超过config大小的batch，产生碎片。调整也能减少system call的复杂度
		// 按时间分批次。非阻塞的时候才会牵扯到生产和IO速度不对称的情况。尽量以batch的方式向broker推送，要么batch满了，要么IO别停在那里发小量的数据，从而产生更多的网络交互
		// ack还会造成一定的差异，0的时候，producer把消息放在到了socket这里就返回了
		conf.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");
		//若干个批次封装成一个Request
		conf.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
		conf.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
		conf.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
		// Batches打包之后先放到 InFlightRequests 然后通过客户端发出去，可以挤压5个没返回ack的请求
		// 单词请求同步阻塞的时候这一个配置无意义。另一种情况是请求的数据量比较大，慢，而broker只返回一个很小的ack，快，则来不及积压，这个配置也会失去意义
		conf.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		/*
		SEND_BUFFER 调整的是内核的 `/proc/sys/net/core/wmem_max`
		RECEIVE_BUFFER 调整的是内核的 `/proc/sys/net/core/rmem_max`
		Kafka用的是Selector、Java nio的SocketChannel，而不是Netty，response过来之后有转圈的线程handle。为什么不用Netty？这里留一个问题
		 */
		conf.setProperty(ProducerConfig.SEND_BUFFER_CONFIG, "32768"); // TCP的缓冲区大小：netstat -natp中的 Send-Q 调整为 -1就会指望OS
		conf.setProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG, "32768"); // TCP的缓冲区大小：netstat -natp中的 Recv-Q 调整为 -1就会指望OS
		return conf;
	}

	public static void main(String[] args) {
		Properties conf = init();
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(conf);
		ProducerRecord<String, String> record = new ProducerRecord<>("ooxx", "hello", "hi");
		Future<RecordMetadata> future = producer.send(record);
		// send()下面紧接着Future.get()的话，send就成了同步的，则batch的空间无法利用，每条必须发走才能继续下一循环
		// 客户端像发送数据之前，要先完成元数据的更新，在waitOnMetadata里
		// 分布式环境下，元数据更新是一件很重要的事情。NetworkClient的第1024行this.metadata.update(...)更新了元数据
		Future<RecordMetadata> send = producer.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {

			}
		});
	}
}
