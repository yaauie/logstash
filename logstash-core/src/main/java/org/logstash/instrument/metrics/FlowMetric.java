package org.logstash.instrument.metrics;

import java.util.Map;

public interface FlowMetric extends Metric<Map<String,Double>> {
    public void capture();

    @Override
    default MetricType getType() { return MetricType.FLOW_RATE; }

    static FlowMetric naive(final String name,
                            final Metric<? extends Number> numeratorMetric,
                            final Metric<? extends Number> denominatorMetric) {
        return new NaiveFlowMetric(name, numeratorMetric, denominatorMetric);
    }

    static FlowMetric extended(final String name,
                               final Metric<? extends Number> numeratorMetric,
                               final Metric<? extends Number> denominatorMetric) {
        return new ExtendedFlowMetric<>(name, numeratorMetric, denominatorMetric);
    }
}
