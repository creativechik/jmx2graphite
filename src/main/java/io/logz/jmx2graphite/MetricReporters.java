package io.logz.jmx2graphite;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.util.concurrent.AbstractService;

public class MetricReporters extends AbstractService {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsPipeline.class);
    private static final String GRAPHITE_HOSTNAME_PROPERTY = "graphite.hostname";
    private static final String GRAPHITE_PORT_PROPERTY = "graphite.port";
    private static final String GRAPHITE_PREFIX_PROPERTY = "graphite.prefix";
    private static final int FAILURES_BEFORE_RESTART = 10;
    private static final int ERROR_CHECK_PERIOD_SECS = 60;

    private static MetricReporters metricReporters;

    private final MetricRegistry registry;
    private final Timer graphiteCheckTimer;

    private GraphiteWithFlushErrorLog graphite;
    private ScheduledReporter reporter;

    private MetricReporters(MetricRegistry registry) {
        this.registry = registry;
        createGraphiteReporter();
        graphiteCheckTimer = new Timer();
        graphiteCheckTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                restartOnTooManyFailures();
            }
        }, TimeUnit.SECONDS.toMillis(ERROR_CHECK_PERIOD_SECS));
    }

    static MetricReporters getInstance(MetricRegistry metricRegistry) {
        if (metricReporters == null) {
            metricReporters = new MetricReporters(metricRegistry);
        }
        return metricReporters;
    }

    private void createGraphiteReporter() {
        graphite = newGraphite();
        reporter = GraphiteReporter.forRegistry(registry)
                .prefixedWith(getPrefix())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
    }

    private void restartOnTooManyFailures() {
        if (graphite != null && graphite.getFlushFailures() >= FAILURES_BEFORE_RESTART) {
            stop();
            createGraphiteReporter();
        }
    }

    private static GraphiteWithFlushErrorLog newGraphite() {
        return new GraphiteWithFlushErrorLog(
                new InetSocketAddress(
                        System.getProperty(GRAPHITE_HOSTNAME_PROPERTY),
                        Integer.parseInt(System.getProperty(GRAPHITE_PORT_PROPERTY, "2003"))
                )
        );
    }

    private static String getPrefix() {
        return System.getProperty(GRAPHITE_PREFIX_PROPERTY);
    }

    @Override
    protected void doStart() {
        reporter.start(10, TimeUnit.SECONDS);
        LOG.info("graphiteReporter started with prefix {}", getPrefix());
        notifyStarted();
    }

    @Override
    protected void doStop() {
        stop();
        notifyStopped();
    }

    private void stop() {
        reporter.stop();
        graphiteCheckTimer.cancel();
        try {
            graphite.close();
        } catch (IOException e) {
            LOG.warn("Error on close graphite", e);
        }
    }
}
