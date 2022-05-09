package com.example.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Test;
import org.springframework.ui.context.Theme;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class SimpleRocketMQ {

    @Test
    public void produce() throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("ooxx");
        producer.setNamesrvAddr("39.107.230.218:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setTopic("wula");
            message.setTags("TagA");
            message.setBody(("ooxx" + i).getBytes());

            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.println(throwable);
                }
            });
        }

        System.in.read();
    }

    @Test
    public void consumePull() throws Exception {

        DefaultLitePullConsumer pullConsumer = new DefaultLitePullConsumer("o1111oxx");
        pullConsumer.setNamesrvAddr("39.107.230.218:9876");
        pullConsumer.start();

        System.out.println("queue");
        Collection<MessageQueue> messageQueues = pullConsumer.fetchMessageQueues("wula");
        messageQueues.forEach(messageQueue -> System.out.println(messageQueue));

        System.out.println("poll...");
        pullConsumer.assign(messageQueues);
        List<MessageExt> poll = pullConsumer.poll();
        poll.forEach(po -> System.out.println(po));

        System.in.read();

    }

    @Test
    public void consumePush() throws Exception {
        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer("ooxx");
        pushConsumer.setNamesrvAddr("39.107.230.218:9876");
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        pushConsumer.subscribe("wula", "*");

        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            // 批量
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                msgs.forEach( msgExt -> {
                    System.out.println(new String(msgExt.getBody()));
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        pushConsumer.start();

        System.in.read();
    }

    @Test
    public void admine() throws Exception {

        DefaultMQAdminExt adminExt = new DefaultMQAdminExt();
        adminExt.setNamesrvAddr("39.107.230.218:9876");
        adminExt.start();

        TopicList topicList = adminExt.fetchAllTopicList();
        Set<String> sets = topicList.getTopicList();
        sets.forEach( s -> System.out.println(s));

        System.out.println("-------");
        TopicRouteData wula = adminExt.examineTopicRouteInfo("wula");
        System.out.println(wula);

    }
}
