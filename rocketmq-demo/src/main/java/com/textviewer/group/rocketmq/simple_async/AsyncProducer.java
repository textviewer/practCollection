package com.textviewer.group.rocketmq.simple_async;

import com.textviewer.group.rocketmq.common.Constant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by Enzo Cotter on 2022/11/8.
 */
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("Jodie_Daily_test");
        // 如果这里不设置,那么会读取环境变量里面的 NAMESRV_ADDR
        producer.setNamesrvAddr(Constant.DEFAULT_NAMESRVADDR);
        producer.start();

        // 失败的重试次数
        producer.setRetryTimesWhenSendAsyncFailed(0);

        int messageCount = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        for (int i = 0; i < messageCount; i++) {
            try {
                final int index = i;
                Message msg = new Message("Jodie_topic_1023",
                        "TagA",
                        "OrderID188",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg, new SendCallback() {

                    @Override
                    public void onSuccess(SendResult sendResult) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d Exception %s %n", index, throwable);
                        throwable.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.shutdown();
    }
}
