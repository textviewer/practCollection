package com.textviewer.group.rocketmq.simple;

import com.textviewer.group.rocketmq.common.Constant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * Created by Enzo Cotter on 2022/11/9.
 */
public class OnewayProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
        // Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // Specify name server addresses.
        producer.setNamesrvAddr(Constant.DEFAULT_NAMESRVADDR);
        // Launch the instance.
        producer.start();
        for (int i = 0; i < 100; i++) {
            // Create a message instance, specifying topic, tag and message body
            Message msg = new Message(Constant.TOPIC, Constant.TAG,
                    ("Hello RocketMQ one_way_producer").getBytes(RemotingHelper.DEFAULT_CHARSET));

            // call send message to deliver message to one of brokers.
            producer.sendOneway(msg);
        }

        // Wait for sending to complete.
        Thread.sleep(5000);
        producer.shutdown();


    }
}
