package org.apache.flume.sink;

import org.apache.flume.channel.file.Channel;
import org.apache.flume.channel.file.Event;
import org.apache.flume.channel.file.LifecycleAware;
import org.apache.flume.channel.file.LifecycleState;
import org.apache.flume.channel.file.Transaction;

import java.util.ArrayList;
import java.util.List;

public class FlumeSink implements LifecycleAware {

    private Channel channel;
    private int maxSize;
    private DataCall call;

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public FlumeSink(int maxSize, DataCall call) {
        this.maxSize = maxSize;
        this.call = call;
    }

    private void take() {
        List<byte[]> list = new ArrayList<>();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event take;
            do {
                take = channel.take();
                if (null == take) {
                    if (list.isEmpty()) {
                        synchronized (channel) {
                            channel.wait();
                        }
                        break;
                    }
                    call.call(list);
                    break;
                }
                list.add(take.getBody());
                if (list.size() >= maxSize) {
                    call.call(list);
                    break;
                }
            } while (null != take);
            transaction.commit();
        } catch (Throwable e) {
            transaction.rollback();
        } finally {
            transaction.close();
        }
    }

    @Override
    public void start() {
        new Thread(() -> {
            while (true) {
                take();
            }
        }).start();
    }

    @Override
    public void stop() {

    }

    @Override
    public LifecycleState getLifecycleState() {
        return null;
    }

    public interface DataCall {

        void call(List<byte[]> list) throws Exception;
    }
}
