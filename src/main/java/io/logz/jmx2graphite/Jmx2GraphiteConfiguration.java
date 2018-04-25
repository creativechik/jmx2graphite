package io.logz.jmx2graphite;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import com.typesafe.config.Config;

/**
 * @author amesika
 */
public class Jmx2GraphiteConfiguration {
    
    private String jolokiaFullUrl;

    /* host of the sampled service */
    private String serviceHost = null;

    /* Metrics polling interval in seconds */
    private int metricsPollingIntervalInSeconds;

    // Which client should we use
    private MetricClientType metricClientType;

    public enum MetricClientType {
        JOLOKIA,
        MBEAN_PLATFORM
    }

    public Jmx2GraphiteConfiguration(Config config) throws IllegalConfiguration {
        if (config.hasPath("service.host")) {
            serviceHost = config.getString("service.host");
        }

        if (config.hasPath("service.poller.jolokia")) {
            metricClientType = MetricClientType.JOLOKIA;
        }
        else if (config.hasPath("service.poller.mbean-direct")) {
            metricClientType = MetricClientType.MBEAN_PLATFORM;
        }

        if (this.metricClientType == MetricClientType.JOLOKIA) {
            jolokiaFullUrl = config.getString("service.poller.jolokia.jolokiaFullUrl");
            String jolokiaHost;
            try {
                URL jolokia = new URL(jolokiaFullUrl);
                jolokiaHost = jolokia.getHost();
            } catch (MalformedURLException e) {
                throw new IllegalConfiguration("service.jolokiaFullUrl must be a valid URL. Error = " + e.getMessage());
            }

            // Setting jolokia url as default
            if (serviceHost == null) {
                serviceHost = jolokiaHost;
            }

        } else if (this.metricClientType == MetricClientType.MBEAN_PLATFORM) {

            // Try to find hostname as default to serviceHost in case it was not provided
            if (serviceHost == null) {
                try {
                    serviceHost = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException e) {
                    throw new IllegalConfiguration("service.host was not defined, and could not determine it from the servers hostname");
                }
            }
        }

        metricsPollingIntervalInSeconds = config.getInt("metricsPollingIntervalInSeconds");
    }

    public String getJolokiaFullUrl() {
        return jolokiaFullUrl;
    }

    public void setJolokiaFullUrl(String jolokiaFullUrl) {
        this.jolokiaFullUrl = jolokiaFullUrl;
    }

    public String getServiceHost() {
        return serviceHost;
    }

    public void setServiceHost(String serviceHost) {
        this.serviceHost = serviceHost;
    }

    public int getMetricsPollingIntervalInSeconds() {
        return metricsPollingIntervalInSeconds;
    }

    public void setMetricsPollingIntervalInSeconds(int metricsPollingIntervalInSeconds) {
        this.metricsPollingIntervalInSeconds = metricsPollingIntervalInSeconds;
    }

    public MetricClientType getMetricClientType() {
        return metricClientType;
    }

    public void setMetricClientType(MetricClientType metricClientType) {
        this.metricClientType = metricClientType;
    }
}
