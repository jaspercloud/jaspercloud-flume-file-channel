package org.apache.flume.channel.file;

import org.apache.flume.agent.FlumeAgent;
import org.apache.flume.sink.FlumeSink;
import org.apache.flume.source.FlumeSource;
import org.junit.Test;

import java.nio.channels.MulticastChannel;
import java.sql.Time;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ChannelTest {

    @Test
    public void writeTest() {
//        SpillableMemoryChannel channel = new SpillableMemoryChannel("agent", new Context());
        FileChannel channel = new FileChannel("agent", new Context());
        channel.start();
        System.out.println("running");

//        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
//        scheduledExecutorService.scheduleAtFixedRate(() -> {
//            long ct = counter.getAndSet(0);
//            System.out.println(ct);
//        }, 0, 1000, TimeUnit.MILLISECONDS);

        while (true) {
            AtomicLong counter = new AtomicLong();
            Transaction transaction = channel.getTransaction();
            transaction.begin();
            long start = System.currentTimeMillis();
            do {
                channel.put(new SimpleEvent("test".getBytes()));
                counter.incrementAndGet();
                if (counter.get() >= 1000) {
                    break;
                }
            } while (System.currentTimeMillis() - start < 1000);
            transaction.commit();
            transaction.close();
            System.out.println("time: " + (System.currentTimeMillis() - start) + " " + counter.get());
        }
    }

    @Test
    public void agentTest() throws Exception {
        FlumeSource flumeSource = new FlumeSource();
        FlumeAgent flumeAgent = new FlumeAgent();
        flumeAgent.setFlumeSource(flumeSource);
        flumeAgent.setFlumeSink(new FlumeSink(10, new FlumeSink.DataCall() {
            @Override
            public void call(List<byte[]> list) throws Exception {
                System.out.println();
            }
        }));
        flumeAgent.setChannel(new FileChannel("agent", new Context()));
        flumeAgent.start();

        while (true) {
            flumeSource.put(Arrays.asList(new byte[3]));
            Thread.sleep(5000);
        }
    }

    @Test
    public void agent() {
        SpillableMemoryChannel channel = new SpillableMemoryChannel("agent", new Context());
        channel.start();
        {
            Transaction transaction = channel.getTransaction();
            transaction.begin();
            channel.put(new SimpleEvent("test".getBytes()));
            transaction.commit();
            transaction.close();
        }
        {
            Transaction transaction = channel.getTransaction();
            transaction.begin();
            Event take = channel.take();
            transaction.rollback();
            transaction.close();
        }

        {
            Transaction transaction = channel.getTransaction();
            transaction.begin();
            Event take = channel.take();
            transaction.commit();
            transaction.close();
        }
    }

    @Test
    public void test() {
        FileChannel fileChannel = new FileChannel("test", new Context());
        fileChannel.start();
        {
            Transaction transaction = fileChannel.getTransaction();
            transaction.begin();
            fileChannel.put(new SimpleEvent("test".getBytes()));
            transaction.commit();
            transaction.close();
        }
        {
            Transaction transaction = fileChannel.getTransaction();
            transaction.begin();
            Event take = fileChannel.take();
            transaction.commit();
            transaction.close();
        }
        System.out.println();
    }

    @Test
    public void channelTest() throws Exception {
        Map<String, String> map = new HashMap<>();
        SpillableMemoryChannel channel = new SpillableMemoryChannel("test", new Context(map));
        channel.start();

        {
            Transaction transaction = channel.getTransaction();
            transaction.begin();
            channel.put(new SimpleEvent("test".getBytes()));
            transaction.commit();
            transaction.close();
        }
        {
            Transaction transaction = channel.getTransaction();
            transaction.begin();
            Event take = channel.take();
            transaction.commit();
            transaction.close();
            System.out.println();
        }

    }
}
