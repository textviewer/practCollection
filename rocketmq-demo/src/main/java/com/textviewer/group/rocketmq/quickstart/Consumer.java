package com.textviewer.group.rocketmq.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * Created by Enzo Cotter on 2022/11/8.
 */
public class Consumer {
    private static final String CONSUMER_GROUP = "please_rename_unique_group_name_4";
    private static final String DEFAULT_NAMESRVADDR = "192.168.88.128:9876";
    private static final String TOPIC = "TopicTest";

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);

        // 如果不设置, 那么会默认读取环境变量 NAMESRV_ADDR
        consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe(TOPIC, "*");

        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
