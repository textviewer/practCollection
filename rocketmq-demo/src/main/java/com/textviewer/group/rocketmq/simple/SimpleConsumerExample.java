//package com.textviewer.group.rocketmq.simple;
//
//import org.apache.rocketmq.client.apis.ClientConfiguration;
//import org.apache.rocketmq.client.apis.ClientException;
//import org.apache.rocketmq.client.apis.ClientServiceProvider;
//import org.apache.rocketmq.client.apis.consumer.FilterExpression;
//import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
//import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
//import org.apache.rocketmq.client.apis.message.MessageView;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.time.Duration;
//import java.util.Collections;
//import java.util.List;
//
///**
// * Created by Enzo Cotter on 2022/11/5.
// */
//public class SimpleConsumerExample {
//    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumerExample.class);
//
//    public SimpleConsumerExample() {
//    }
//
//    public static void main(String[] args) throws ClientException {
//        ClientServiceProvider provider = ClientServiceProvider.loadService();
//        String endpoints = "localhost:8081";
//        String topic = "TestTopic";
//        FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);
//        ClientConfiguration configuration = ClientConfiguration.newBuilder().setEndpoints(endpoints).build();
//        SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
//                // 设置消费者
//                .setConsumerGroup("Your ConsumerGroup")
//                // 设置接入点
//                .setClientConfiguration(configuration)
//                // 设置绑定的订阅关系
//                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression)).build();
//        List<MessageView> messageViewList;
//        try {
//            // SimpleConsume需要主动获取消息, 并处理
//            messageViewList = simpleConsumer.receive(10, Duration.ofSeconds(30));
//            messageViewList.forEach(messageView -> {
//                System.out.println(messageView);
//                // 消费处理王朝后, 需要主动调用ACK提交消费结果
//                try {
//                    simpleConsumer.ack(messageView);
//                } catch (ClientException e) {
//                    e.printStackTrace();
//                }
//            });
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
