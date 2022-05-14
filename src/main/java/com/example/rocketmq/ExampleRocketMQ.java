package com.example.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExampleRocketMQ {

    @Test
    public void admin() throws Exception {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt();
        adminExt.setNamesrvAddr("39.107.230.218:9876");
        adminExt.start();

        ClusterInfo clusterInfo = adminExt.examineBrokerClusterInfo();
        HashMap<String, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();
        Set<Map.Entry<String, BrokerData>> entries = brokerAddrTable.entrySet();
        Iterator<Map.Entry<String, BrokerData>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, BrokerData> next = iterator.next();
            System.out.println(next.getKey() + " " + next.getValue());
        }

        TopicList topicList = adminExt.fetchAllTopicList();
        topicList.getTopicList().forEach(s->{
            System.out.println(s);
        });
    }

    /*---------------------001--------------------*/
    /**
     * 无序消息
     * @throws Exception
     */
    @Test
    public void consumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("xxxx");
        consumer.setNamesrvAddr("39.107.230.218:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("bala", "*");
        consumer.setMaxReconsumeTimes(2);
        consumer.setPullBatchSize(2); // 从broker拉取消息的数量
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt messageExt = msgs.get(0);
                try{
                    System.out.println(new String(messageExt.getBody()));
                    if (messageExt.getKeys().equals("key:2")){
                        int a = 1/0;
                    }
                }catch (Exception e){
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
        consumer.shutdown();
    }

    @Test
    public void product() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ooxx");
        producer.setNamesrvAddr("39.107.230.218:9876");
        producer.start();

        List<Message> msgs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = new Message(
                    "bala",
                    "tagA",
                    "key:" + i,
                    ("message:" + i).getBytes()
            );
            msgs.add(message);
        }
        SendResult send = producer.send(msgs);
        System.out.println(send);
    }


    /*---------------------002--------------------*/

    /**
     * 有序消息
     */
    @Test
    public void orderProduce() throws Exception {

        TransactionMQProducer producer = new TransactionMQProducer("topic_transaction2");
        producer.setNamesrvAddr("39.107.230.218:9876");
        producer.start();

        MyMessageQueueSelector selector = new MyMessageQueueSelector();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("order_topic",
                    "TagA",
                    ("message body : " + i + " type : " + i % 3).getBytes()
            );
            SendResult result = producer.send(message, selector, i % 3);
            System.out.println(result);
        }

    }

    @Test
    public void orderConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("xoxo");
        consumer.setNamesrvAddr("39.107.230.218:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("order_topic", "*");

        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                msgs.forEach(msgse -> {
                    System.out.println(Thread.currentThread().getName() + " : " + new String(msgse.getBody()) );
                });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
        System.in.read();
        consumer.shutdown();

    }


    /*---------------------003--------------------*/

    /**
     * 延迟消息
     */
    @Test
    public void consumeDelay() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("delayconsumer");
        consumer.setNamesrvAddr("39.107.230.218:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("topic_delay", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                msgs.forEach( msg -> {
                    System.out.println(msg);
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    @Test
    public void producerDelay() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("delay_producer");
        producer.setNamesrvAddr("39.107.230.218:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message(
                    "topic_delay",
                    "TagD",
                    ("message " + i).getBytes()
            );
            message.setDelayTimeLevel(i%18);
            SendResult send = producer.send(message);
            System.out.println(send);
        }
        System.in.read();
    }


    /*---------------------004--------------------*/


    @Test
    public void consumerTransaction() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tranaction_consumer2");
        consumer.setNamesrvAddr("39.107.230.218:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("topic_transaction2", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                msgs.forEach(msg -> System.out.println(msg));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    @Test
    public void producerTransaction() throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("transaction_producer2");
        producer.setNamesrvAddr("39.107.230.218:9876");
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                // send ok  half半消息

                String action = (String) arg;
                String transactionId = msg.getTransactionId();
                System.out.println("transactionID:" + transactionId);
                switch (action){
                    case "0":
                        System.out.println(Thread.currentThread().getName() + "send half:Async api call...action : 0");
                        return LocalTransactionState.UNKNOW;
                    case "1":
                        System.out.println(Thread.currentThread().getName() + "send half:localTransaction faild...action : 1");
                        return LocalTransactionState.ROLLBACK_MESSAGE;
                    case "2":
                        System.out.println(Thread.currentThread().getName() + "send half:localTransaction commit...action : 2");
                        try {
                            Thread.sleep(1000*10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        return LocalTransactionState.COMMIT_MESSAGE;
                }
                return null;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                // call back check

                String transactionId = msg.getTransactionId();
                String action = msg.getProperty("action");
                System.out.println("transactionID_check:" + transactionId);
                switch (action){
                    case "0":
                        System.out.println(Thread.currentThread().getName() + "check: action :0 UNKNOW");
                        return LocalTransactionState.COMMIT_MESSAGE;
                    case "1":
                        System.out.println(Thread.currentThread().getName() + "check: action :1 ROLLBACK");
                        return LocalTransactionState.ROLLBACK_MESSAGE;
                    case "2":
                        System.out.println(Thread.currentThread().getName() + "check: action :2 COMMIT");
                        return LocalTransactionState.COMMIT_MESSAGE;
                }
                return null;
            }
        });
        // need thread 不是必须要配置
        producer.setExecutorService(new ThreadPoolExecutor(
                1,
                Runtime.getRuntime().availableProcessors(),
                2000,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Transaction thread");
                    }
                }
        ));
        producer.start();
        /*for (int i = 0; i < 10; i++) {
            Message msgt = new Message(
                    "topic_transaction2",
                    "tagA",
                    "key:" + i,
                    ("message : " + i).getBytes()
            );
            msgt.putUserProperty("action", i%3 + "");
            // 发送的是半消息
            TransactionSendResult res = producer.sendMessageInTransaction(msgt, i % 3 + "");
            System.out.println(res);
        }*/
        System.in.read();
    }
}
