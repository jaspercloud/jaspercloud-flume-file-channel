package org.apache.flume.channel.file;

public class FlumeException extends RuntimeException {

    public FlumeException() {
    }

    public FlumeException(String message) {
        super(message);
    }

    public FlumeException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlumeException(Throwable cause) {
        super(cause);
    }
}
