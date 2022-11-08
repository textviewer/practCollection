package com.textviewer.group.rocketmq.quickstart;

import com.textviewer.group.rocketmq.common.Constant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * Created by Enzo Cotter on 2022/11/8.
 */
public class Consumer {


    // 推模式,默认等待接收数据,接收到了之后, 执行listener
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(Constant.CONSUMER_GROUP);

        // 如果不设置, 那么会默认读取环境变量 NAMESRV_ADDR
        consumer.setNamesrvAddr(Constant.DEFAULT_NAMESRVADDR);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe(Constant.TOPIC, "*");

        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
