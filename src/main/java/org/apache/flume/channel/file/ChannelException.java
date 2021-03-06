package org.apache.flume.channel.file;

/**
 * <p>A channel exception is raised whenever a channel operation fails.</p>
 */
public class ChannelException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * @param message the exception message
     */
    public ChannelException(String message) {
        super(message);
    }

    /**
     * @param ex the causal exception
     */
    public ChannelException(Throwable ex) {
        super(ex);
    }

    /**
     * @param message the exception message
     * @param ex the causal exception
     */
    public ChannelException(String message, Throwable ex) {
        super(message, ex);
    }

}
