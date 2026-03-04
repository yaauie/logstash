package org.logstash.instrument.metrics;

public interface MetricNode extends co.elastic.logstash.api.Metric {

    @Override
    public NamespacedMetric namespace(String... key);
}
