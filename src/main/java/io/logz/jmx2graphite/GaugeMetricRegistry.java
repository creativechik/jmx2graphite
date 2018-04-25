package io.logz.jmx2graphite;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

public class GaugeMetricRegistry extends MetricRegistry {
    @SuppressWarnings("unchecked")
    @Override
    public <T extends Metric> T register(String name, T metric) throws IllegalArgumentException {
        Gauge existingGauge = getGauges().get(name);
        return existingGauge != null ? (T) existingGauge : super.register(name, metric);
    }
}
