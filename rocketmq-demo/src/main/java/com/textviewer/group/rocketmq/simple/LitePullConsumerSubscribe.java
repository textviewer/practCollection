package com.textviewer.group.rocketmq.simple;

import com.textviewer.group.rocketmq.common.Constant;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by Enzo Cotter on 2022/11/10.
 *
 * Lite pull subscribe模式
 */
public class LitePullConsumerSubscribe {
    public static volatile boolean running = true;

    public static void main(String[] args) throws MQClientException {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer("lite_pull_consumer_test");
        litePullConsumer.subscribe(Constant.TOPIC,"*");
        litePullConsumer.setPullBatchSize(20);
        litePullConsumer.start();
        try {
            while (running) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                System.out.printf("%s%n", messageExts);
            }
        } finally {
            litePullConsumer.shutdown();
        }
    }
}
