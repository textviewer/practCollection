package com.textviewer.group.rocketmq.simple;

import com.textviewer.group.rocketmq.common.Constant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Enzo Cotter on 2022/11/13.
 */
public class PushConsumer {
    public static void main(String[] args) throws MQClientException {
        // 初始化consumer, 并设置consumer group name
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");

        // 设置NameServer地址
        consumer.setNamesrvAddr(Constant.DEFAULT_NAMESRVADDR);
        // 订阅一个或多个TOPIC, 并指定tag过滤条件, 这里指定*表示接收所有tag的消息
        consumer.subscribe(Constant.TOPIC, "*");
        // 默认是集群模式
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 可以改成广播模式
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 注册回调接口来处理从Broker中收到的消息
        // 并发消费
        concurrentlyModel(consumer);
        // 顺序消费

        // 启动Consumer
        consumer.start();
        System.out.println("Consumer Started.%n");
    }

    // 并发消费
    private static void concurrentlyModel(DefaultMQPushConsumer consumer) {
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                // 返回消息消费状态, ConsumeConcurrentlyStatus.CONSUME_SUCCESS为消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
    }

    // 顺序消费
    private static void OrderlyModel(DefaultMQPushConsumer consumer) {
        consumer.registerMessageListener(new MessageListenerOrderly() {
            AtomicLong consumeTimes = new AtomicLong(0);
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), list);
                this.consumeTimes.incrementAndGet();
                if (this.consumeTimes.get() % 2 == 0) {
                    return ConsumeOrderlyStatus.SUCCESS;
                } else if (this.consumeTimes.get() % 5 == 0) {
                    consumeOrderlyContext.setSuspendCurrentQueueTimeMillis(3000);
                    // ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT表示消费失败
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
    }
}
