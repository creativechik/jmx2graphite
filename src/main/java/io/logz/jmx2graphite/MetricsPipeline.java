package io.logz.jmx2graphite;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;

/**
 * @author amesika
 */

public class MetricsPipeline {
    private static final Logger logger = LoggerFactory.getLogger(MetricsPipeline.class);
    private static final MetricRegistry METRIC_REGISTRY = new GaugeMetricRegistry();

    private int pollingIntervalSeconds;

    private MBeanClient client;

    public MetricsPipeline(Jmx2GraphiteConfiguration conf, MBeanClient client) {
        this.client = client;
        this.pollingIntervalSeconds = conf.getMetricsPollingIntervalInSeconds();

        MetricReporters metricReporters = MetricReporters.getInstance(METRIC_REGISTRY);
        if (!metricReporters.isRunning()) {
            metricReporters.startAsync();
        }
    }

    private List<MetricValue> poll() {
        try {
            long pollingWindowStartSeconds = getPollingWindowStartSeconds();
            Stopwatch sw = Stopwatch.createStarted();
            List<MetricBean> beans = client.getBeans();
            logger.info("Found {} metric beans. Time = {}ms, for {}", beans.size(),
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    new Date(TimeUnit.SECONDS.toMillis(pollingWindowStartSeconds)));

            sw.reset().start();
            List<MetricValue> metrics = client.getMetrics(beans);
            logger.info("metrics fetched. Time: {} ms; Metrics: {}", sw.stop().elapsed(TimeUnit.MILLISECONDS), metrics.size());
            if (logger.isTraceEnabled()) printToFile(metrics);
            return changeTimeTo(pollingWindowStartSeconds, metrics);

        } catch (MBeanClient.MBeanClientPollingFailure e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed polling metrics from client ({}): {}", client.getClass().toString(), e.getMessage(), e);
            } else {
                logger.warn("Failed polling metrics from client ({}): {}", client.getClass().toString(), e.getMessage());
            }

            return Collections.emptyList();
        }
    }

    public void pollAndSend() {
        try {
            List<MetricValue> metrics = poll();
            Stopwatch sw = Stopwatch.createStarted();
            metrics.forEach(m -> METRIC_REGISTRY.register(m.getName(), (Gauge<Number>) m::getValue));
            logger.info("{} metrics sent to Graphite. Time: {} ms",
                    metrics.size(),
                    sw.stop().elapsed(TimeUnit.MILLISECONDS)
            );
        } catch (IllegalArgumentException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Metric registration failed: " + e.getMessage(), e);
            } else {
                logger.warn("Metric registration failed: " + e.getMessage());
            }
        } catch (Throwable t) {
            logger.error("Unexpected error occured while polling and sending. Error = {}", t.getMessage(), t);
            // not throwing out since the scheduler will stop in any exception
        }
    }

    private long getPollingWindowStartSeconds() {
        long now = System.currentTimeMillis();
        long pollingIntervalMs = TimeUnit.SECONDS.toMillis(pollingIntervalSeconds);
        return TimeUnit.MILLISECONDS.toSeconds(now - (now % pollingIntervalMs));
    }

    private void printToFile(List<MetricValue> metrics) {
        for (MetricValue v : metrics) {
            logger.trace(v.toString());
        }
    }

    private List<MetricValue> changeTimeTo(long newTime, List<MetricValue> metrics) {
        return metrics.stream()
                .map(m -> new MetricValue(m.getName(), m.getValue(), newTime))
                .collect(Collectors.toList());
    }
}
