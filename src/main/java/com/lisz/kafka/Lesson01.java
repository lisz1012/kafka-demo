package com.lisz.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
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
		props.setProperty(ProducerConfig.ACKS_CONFIG, "-1"); //默认值是1. 0是放到socket缓冲区就走，all是所有isr都确认了才走

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		//producer就是一个提供者，面向的其实是Broker，虽然在使用的时候，我们期望的是把数据打入topic
		/*
		msb-items
		三种商品，每种商品有线性的3个ID，相同的key最好去到同一个分区
		 */

		for (int i = 0; i < 30; i++) {
			for (int j = 0; j < 3; j++) {
				ProducerRecord<String, String> record
						= new ProducerRecord<>(topic, "item" + j, "val" + i);
				Future<RecordMetadata> send = producer.send(record);
				RecordMetadata rm = send.get();
				int partition = rm.partition();
				long offset = rm.offset();
				System.out.println("key: " + record.key() + " val: " + record.value()
						+ " partition: " + partition + " offset: " + offset
						+ " timestamp: " + rm.timestamp());
				Thread.sleep(1000);
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
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g1");
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
		// Kafka的Consumer回动态负载均衡：当Consumer个数有变化的时候可能会让出或者得到某些分区
		consumer.subscribe(Arrays.asList("msb-items"), new ConsumerRebalanceListener() {
			// 懒执行，不poll的时候不会执行
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				System.out.println("---onPartitionsRevoked");
				for (TopicPartition tp : partitions) {
					System.out.println(tp.partition());
				}
			}

			// 懒执行，不poll的时候不会执行
			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				System.out.println("---onPartitionsAssigned");
				for (TopicPartition tp : partitions) {
					System.out.println(tp.partition());
				}
			}
		});


		Map<TopicPartition, Long> tts = new HashMap<>();
		Set<TopicPartition> assignment = consumer.assignment();
		while (assignment.isEmpty()) { // 是个回调，执行到下面的时候可能还没有assign上
			consumer.poll(Duration.ofMillis(100));
			assignment = consumer.assignment();
		}

		//自己填充HashMap，为每个分区倒退时间
		/*
			1.通过时间戳换算出offset，再通过seek来自定义偏移。seek是最值钱的
			2。如果自己维护offset维护持久化。通过seek完成定点取
		 */
		for (TopicPartition topicPartition : assignment) {
			tts.put(topicPartition, 1623990624536L);
		}
		// 通过consumer的api取回timeindex的数据
		Map<TopicPartition, OffsetAndTimestamp> offsetTime = consumer.offsetsForTimes(tts);
		for (TopicPartition topicPartition : assignment) {
			//按照时间换算offset，本质跟从Redis或者MySQL中读取回来是一样的。
			//seek产生的空洞会被Kafka认为其中的数据已经消费，LAG=0
			long offset = offsetTime.get(topicPartition).offset(); // 因为被设定的时间之后可能没有数据
			System.out.println("Offset: " + offset);
			consumer.seek(topicPartition, offset); //通过seek修正偏移
		}

//		try {
//			System.in.read();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}


		while (true) {
			// 0-n 条, 微批的感觉
			/*
			常识：如果想多线程处理多分区
			每poll一次用一个语义：一个job启动，
			一个job用多线程并行处理分区，且job应该被控制是串行的
			 */
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			if (!records.isEmpty()) {
				System.out.println("--------------" + records.count() + "--------------");
				Set<TopicPartition> partitions = records.partitions(); //每次取多个分区的数据
				// 且每个分区内的诗句是有序的。
				/*
				如果手动提交offset：
				1。按每条记录（消息进度）同步提交
				2。按分区粒度处理
				3。按当前这一poll
				思考：如果在多线程下：
				1。以上1、3的方式不用多线程
				2。以上2的方式最容易想到多线程，每个分区一个线程来处理，有没有问题？
				   不会啊，不同的分区各自有各自的offset
				多个Consumer（线程）同时消费一个分区有俩问题：
					1。本来partition内有序的消息，处理完可能会乱序
					2。有的线程成功，有的失败，如何提交offset？还是要以批次为粒度维护offset
				Kafka是MQ其业务特点决定了它可以使用磁盘的顺序读写的特性，这一点要比随机读写快很多
				Kafka村数据的时候先存到pagecache（内存中），再异步刷盘，此时如果Consumer来拉取数据，则会很快
				Kafka保存数据的同时，还保存各条数据的索引，因为每一条数据的大小不同，也不是每条都写索引，而是分段，像跳表
				还有通过时间戳（默认是producer生产的时间）的换算到索引offset再取数据，本质还是按照时间戳查询
				由于Kafka自身不加工数据，而是直接原样发走，所以她可以直接sendfile，减少了内核到app拷贝数据，再从app拷贝回内核的过程
				ack=-1的时候, 所有的in-sync replicas broker的消息进度是一致的，到一定时候没同步的broker会被踢出ISR。Consumer只能消费到
				指定数量的ISR都同步了的那个位置：High Water Mark
				 */
				for (TopicPartition partition : partitions) {
					List<ConsumerRecord<String, String>> pRecords = records.records(partition);
					// 在一个微批里，按分区获取poll回来的数据
					//线性分区处理，还可以多线程并行按分区处理
					Iterator<ConsumerRecord<String, String>> piter = pRecords.iterator();
					while (piter.hasNext()) {
						ConsumerRecord<String, String> record = piter.next();
						long offset = record.offset();
						long timestamp = record.timestamp();
						System.out.println("key: " + record.key() + " val: " + record.value()
								+ " partition: " + partition.partition() + " offset: " + offset
								+ " timestamp: " + timestamp);

						TopicPartition topicPartition = new TopicPartition(record.topic(), partition.partition());
						OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset + 1);
						Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
						map.put(topicPartition, offsetAndMetadata);
						consumer.commitSync(map); //以一条记录为粒度提交offset, 最安全的
					}
					long poff = pRecords.get(pRecords.size() - 1).offset(); // 拿到最后一条记录的offset, 用来提交 offset
					OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(poff);
					Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
					map.put(partition, offsetAndMetadata);
					consumer.commitSync(map); //分区粒度提交offset。第二种
					/*
					因为你都分区了，拿到了分区的数据集，可能期望的是先对数据整体加工
					小问题会出现：取得最后一条记录的那个偏移量，放到offset里
					Kafka很傻，那拿走多少我不关心，你告诉我最后一个小的offset
					多线程维护两端的offset，当前这一批要么都成功，要么都失败

					 */
				}

				consumer.commitSync(); //一整个poll批次为单位提交offset，第三种

//				Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
//				while (iterator.hasNext()) {
//					// 因为一个consumer可以消费多个分区，但是一个分区只能给一个组里的一个consumer消费
//					ConsumerRecord<String, String> record = iterator.next();
//					int partition = record.partition();
//					long offset = record.offset();
//					System.out.println("key: " + record.key() + " val: " + record.value()
//							+ " partition: " + partition + " offset: " + offset);
//				}
			}
		}
	}

}
