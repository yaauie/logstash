package org.logstash.instrument.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class LazyInstantiatedFlowMetric<N extends Number, D extends Number> implements FlowMetric {

    static final Logger LOGGER = LogManager.getLogger(LazyInstantiatedFlowMetric.class);

    private final String name;

    private final Supplier<Metric<N>> numeratorSupplier;
    private final Supplier<Metric<D>> denominatorSupplier;

    private final AtomicReference<FlowMetric> inner = new AtomicReference<>();

    private static final Map<String,Double> EMPTY_MAP = Map.of();

    public LazyInstantiatedFlowMetric(final String name,
                                      final Supplier<Metric<N>> numeratorSupplier,
                                      final Supplier<Metric<D>> denominatorSupplier) {
        this.name = name;
        this.numeratorSupplier = numeratorSupplier;
        this.denominatorSupplier = denominatorSupplier;
    }

    @Override
    public void capture() {
        final Optional<FlowMetric> inner = getInner();
        if (inner.isPresent()) {
            inner.get().capture();
        } else {
            LOGGER.warn("Underlying metrics for `{}` not yet instantiated, could not capture their rates", this.name);
        }
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public MetricType getType() {
        return MetricType.FLOW_RATE;
    }

    @Override
    public Map<String, Double> getValue() {
        return getInner().map(FlowMetric::getValue).orElse(EMPTY_MAP);
    }

    private Optional<FlowMetric> getInner() {
        return Optional.ofNullable(inner.getPlain()) // avoid volatile reads
                       .or(this::attemptCreateInner);
    }

    private Optional<FlowMetric> attemptCreateInner() {
        final FlowMetric effectiveInner = inner.updateAndGet((existing) -> {
            if (Objects.nonNull(existing)) { return existing; }

            final Metric<N> numeratorMetric = numeratorSupplier.get();
            if (Objects.isNull(numeratorMetric)) { return null; }

            final Metric<D> denominatorMetric = denominatorSupplier.get();
            if (Objects.isNull(denominatorMetric)) { return null; }

            return FlowMetric.create(this.name, numeratorMetric, denominatorMetric);
        });
        return Optional.ofNullable(effectiveInner);
    }
}
