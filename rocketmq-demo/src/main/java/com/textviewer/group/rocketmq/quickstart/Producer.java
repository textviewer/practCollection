package com.textviewer.group.rocketmq.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * Created by Enzo Cotter on 2022/11/7.
 */
public class Producer {
//    public static final int MESSAGE_COUNT = 1000;
    private static final String PRODUCER_GROUP = "Please_rename_unique_group_name";
    private static final String DEFAULT_NAMESRVADDR = "192.168.88.128:9876";
    private static final String TOPIC = "TopicTest";
    private static final String TAG = "TagA";

    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

        // 如果这里不设置,那么会读取环境变量里面的 NAMESRV_ADDR
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        producer.start();

        for (int i = 0; i < 5; i++) {
            try {
                // 创建消息体
                Message msg = new Message(TOPIC, TAG, ("hello rocketmq " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

                SendResult sendResult = producer.send(msg);

                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        producer.shutdown();
    }
}
