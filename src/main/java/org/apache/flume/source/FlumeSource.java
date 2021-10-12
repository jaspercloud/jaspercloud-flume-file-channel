package org.apache.flume.source;

import org.apache.flume.channel.file.Channel;
import org.apache.flume.channel.file.LifecycleAware;
import org.apache.flume.channel.file.LifecycleState;
import org.apache.flume.channel.file.SimpleEvent;
import org.apache.flume.channel.file.Transaction;

import java.util.List;

public class FlumeSource implements LifecycleAware {

    private Channel channel;

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public void put(List<byte[]> list) {
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            for (byte[] item : list) {
                channel.put(new SimpleEvent(item));
            }
            transaction.commit();
        } catch (Throwable e) {
            e.printStackTrace();
            transaction.rollback();
        } finally {
            transaction.close();
        }
        synchronized (channel) {
            channel.notifyAll();
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public LifecycleState getLifecycleState() {
        return null;
    }
}
