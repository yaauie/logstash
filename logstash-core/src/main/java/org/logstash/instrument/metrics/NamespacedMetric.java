package org.logstash.instrument.metrics;

import org.logstash.instrument.metrics.timer.TimerMetric;

public interface NamespacedMetric extends co.elastic.logstash.api.NamespacedMetric, MetricNode {
    @Override
    TimerMetric timer(String name);

    @Override
    MetricNode root();
}
