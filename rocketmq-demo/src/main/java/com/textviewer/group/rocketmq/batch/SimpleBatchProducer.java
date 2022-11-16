package com.textviewer.group.rocketmq.batch;

import com.textviewer.group.rocketmq.common.Constant;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enzo Cotter on 2022/11/14.
 */
public class SimpleBatchProducer {

    public static final String PRODUCER_GROUP = "BatchProducerGroupName";
    public static final String TOPIC = "BatchTest";
    public static final String TAG = "Tag";

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

        producer.setNamesrvAddr(Constant.DEFAULT_NAMESRVADDR);

        producer.start();



        List<Message> messages = new ArrayList<>();
        messages.add(new Message(TOPIC, TAG, "OrderID001", "Hello world 0".getBytes(StandardCharsets.UTF_8)));
        messages.add(new Message(TOPIC, TAG, "OrderID002", "Hello world 1".getBytes(StandardCharsets.UTF_8)));
        messages.add(new Message(TOPIC, TAG, "OrderID003", "Hello world 2".getBytes(StandardCharsets.UTF_8)));

        SendResult sendResult = producer.send(messages);

        System.out.printf("%s", sendResult);

    }
}
