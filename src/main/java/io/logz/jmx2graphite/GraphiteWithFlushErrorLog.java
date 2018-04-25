package io.logz.jmx2graphite;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import com.codahale.metrics.graphite.Graphite;

public class GraphiteWithFlushErrorLog extends Graphite {
    private int flushFailures = 0;

    /**
     * Creates a new client which connects to the given address using the default
     * {@link SocketFactory}.
     *
     * @param address the address of the Carbon server
     */
    public GraphiteWithFlushErrorLog(InetSocketAddress address) {
        super(address);
    }

    public int getFlushFailures() {
        return flushFailures;
    }

    @Override
    public void flush() throws IOException {
        try {
            super.flush();
        } catch (IOException e) {
            flushFailures++;
            throw e;
        }
    }

}
