package org.apache.flume.channel.file;


public class ChannelFullException extends ChannelException {

    private static final long serialVersionUID = -8098141359417449525L;

    /**
     * @param message the exception message
     */
    public ChannelFullException(String message) {
        super(message);
    }

    /**
     * @param ex the causal exception
     */
    public ChannelFullException(Throwable ex) {
        super(ex);
    }

    /**
     * @param message the exception message
     * @param ex      the causal exception
     */
    public ChannelFullException(String message, Throwable ex) {
        super(message, ex);
    }

}
