package org.apache.flume.agent;

import org.apache.flume.channel.file.Channel;
import org.apache.flume.sink.FlumeSink;
import org.apache.flume.source.FlumeSource;

public class FlumeAgent {

    private FlumeSource flumeSource;
    private Channel channel;
    private FlumeSink flumeSink;

    public void setFlumeSource(FlumeSource flumeSource) {
        this.flumeSource = flumeSource;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public void setFlumeSink(FlumeSink flumeSink) {
        this.flumeSink = flumeSink;
    }

    public void start() {
        flumeSource.setChannel(channel);
        flumeSink.setChannel(channel);
        channel.start();
        flumeSink.start();
    }
}
