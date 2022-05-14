package com.example.rocketmq;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class MyMessageQueueSelector implements MessageQueueSelector {


    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        Integer index = (Integer) arg;
        int target = index % mqs.size();
        return mqs.get(target);
    }
}
