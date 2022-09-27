package org.logstash.instrument.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * An {@link ExtendedFlowMetric} is a {@link FlowMetric} that is capable of producing
 * any number of rolling-average rates, each with their own retention policy that includes
 * the minimum resolution at which we allow captures to be tracked and the maximum retention
 * period, the oldest entry of which will be used as a baseline for calculations.
 *
 * @param <N> the numerator metric's value type
 * @param <D> the denominator metric's value type
 */
public class ExtendedFlowMetric<N extends Number,D extends Number> extends AbstractMetric<Map<String,Double>> implements FlowMetric {

    private static final Logger LOGGER = LogManager.getLogger(ExtendedFlowMetric.class);


    private final Metric<N> numeratorMetric;
    private final Metric<D> denominatorMetric;

    //
    private final CaptureFactory captureFactory;

    private final Capture lifetimeBaseline;

    private final List<RollingAverageSeries> seriesList;

    // test dependency injection
    private final LongSupplier nanoTimeSupplier;

    // in order to prevent presenting false-precision, we limit the precision to four digits.
    static final MathContext FOUR_DIGIT_PRECISION = new MathContext(4, RoundingMode.HALF_UP);
    static final String LIFETIME_KEY = "lifetime";

    public ExtendedFlowMetric(final String name,
                              final Metric<N> numeratorMetric,
                              final Metric<D> denominatorMetric) {
        this(System::nanoTime, name, numeratorMetric, denominatorMetric);
    }

    ExtendedFlowMetric(final LongSupplier nanoTimeSupplier,
                       final String name,
                       final Metric<N> numeratorMetric,
                       final Metric<D> denominatorMetric) {
        super(name);
        this.captureFactory = CaptureFactory.optimizedFor(numeratorMetric, denominatorMetric);

        this.nanoTimeSupplier = nanoTimeSupplier;
        this.numeratorMetric = numeratorMetric;
        this.denominatorMetric = denominatorMetric;

        this.lifetimeBaseline = doCapture();

        this.seriesList = Arrays.stream(BuiltInRetentionPolicy.values())
                                .map(this::retentionSeriesFromRetentionPolicy)
                                .collect(Collectors.toList());
    }

    /**
     * The {@link CaptureFactory} creates {@link Capture}s
     */
    @FunctionalInterface
    interface CaptureFactory {
        <NN extends Number, DD extends Number> Capture createCapture(final long nanoTimestamp, final NN numerator, final DD denominator);

        /**
         * Return an
         * @param numeratorMetric the numerator metric
         * @param denominatorMetric the denominator metric
         * @return an {@link CaptureFactory} that is capable of producing optimized {@link Capture}s for the given metrics.
         */
        private static CaptureFactory optimizedFor(final Metric<? extends Number> numeratorMetric,
                                                   final Metric<? extends Number> denominatorMetric) {
            Objects.requireNonNull(numeratorMetric, "numeratorMetric");
            Objects.requireNonNull(denominatorMetric, "denominatorMetric");

            if (numeratorMetric.getType() == MetricType.COUNTER_LONG) {
                if (denominatorMetric.getType() == MetricType.COUNTER_LONG) {
                    LOGGER.debug("[{}({})/{}({})] -> LongLongCapture", numeratorMetric.getName(), numeratorMetric.getType(), denominatorMetric.getName(), denominatorMetric.getType());
                    return LongLongCapture::new;
                }
                LOGGER.debug("[{}({})/{}({})] -> LongNumberCapture", numeratorMetric.getName(), numeratorMetric.getType(), denominatorMetric.getName(), denominatorMetric.getType());
                return LongNumberCapture::new;
            }
            LOGGER.debug("[{}({})/{}({})] -> NumberNumberCapture", numeratorMetric.getName(), numeratorMetric.getType(), denominatorMetric.getName(), denominatorMetric.getType());
            return NumberNumberCapture::new;
        }
    }

    private RollingAverageSeries retentionSeriesFromRetentionPolicy(BuiltInRetentionPolicy builtInRetentionPolicy) {
        return new RollingAverageSeries(builtInRetentionPolicy, this.lifetimeBaseline);
    }

    private Capture doCapture() {
        return captureFactory.createCapture(nanoTimeSupplier.getAsLong(), numeratorMetric.getValue(), denominatorMetric.getValue());
    }

    @Override
    public void capture() {
        final Capture candidateCapture = doCapture();
        this.seriesList.forEach(series -> series.append(candidateCapture));
    }

    @Override
    public Map<String, Double> getValue() {
        final Capture currentCapture = doCapture();

        final Map<String, Double> rates = new LinkedHashMap<>();

        this.seriesList.forEach(series -> series.baseline(currentCapture.nanoTime())
                                                .map((b) -> getRate(currentCapture, b))
                                                .orElseGet(OptionalDouble::empty)
                                                .ifPresent((rate) -> rates.put(series.policy.toString(), rate)));

        getRate(currentCapture, lifetimeBaseline).ifPresent((rate) -> rates.put(LIFETIME_KEY, rate));

        return Collections.unmodifiableMap(rates);
    }

    private OptionalDouble getRate(final Capture current, final Capture baseline) {
        Objects.requireNonNull(baseline, "baseline");
        if (baseline == current) { return OptionalDouble.empty(); }

        final BigDecimal deltaNumerator = current.numerator().subtract(baseline.numerator());
        final BigDecimal deltaDenominator = current.denominator().subtract(baseline.denominator());

        if (deltaDenominator.signum() == 0) {
            return OptionalDouble.empty();
        }

        return OptionalDouble.of(deltaNumerator.divide(deltaDenominator, 5, RoundingMode.FLOOR)
                                               .round(FOUR_DIGIT_PRECISION)
                                               .doubleValue());
    }

    /**
     * A {@link Capture} holds a pair of point-in-time data-points, which can be compared
     * to produce a rate-of change of the numerator relative to the denominator.
     */
    interface Capture {
        long nanoTime();
        BigDecimal numerator();
        BigDecimal denominator();
    }

    /**
     * The {@link AbstractCapture} is an abstract implementation of {@link Capture}
     * that holds common timestamp-related information so that the {@link RollingAverageSeries}
     * can properly retain its configured resolution and retention.
     */
    private static abstract class AbstractCapture implements Capture {
        private final long nanoTimestamp;

        public AbstractCapture(long nanoTimestamp) {
            this.nanoTimestamp = nanoTimestamp;
        }

        @Override
        public long nanoTime() {
            return nanoTimestamp;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() +"{" +
                    "nanoTimestamp=" + nanoTimestamp +
                    " numerator=" + numerator() +
                    " denominator=" + denominator() +
                    '}';
        }
    }

    /**
     * An unoptimized, fully-boxed implementation of {@link Capture}.
     *
     * @param <NN> the numerator's type
     * @param <DD> the denominator's type
     */
    private static class NumberNumberCapture<NN extends Number, DD extends Number> extends AbstractCapture {

        private final NN numerator;
        private final DD denominator;

        NumberNumberCapture(final long nanoTimestamp,
                            final NN numerator,
                            final DD denominator) {
            super(nanoTimestamp);
            this.numerator = numerator;
            this.denominator = denominator;
        }

        @Override
        public BigDecimal numerator() {
            return BigDecimal.valueOf(numerator.doubleValue());
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(denominator.doubleValue());
        }
    }

    /**
     * A common abstract class for {@link Capture}s whose numerator is a long,
     * optimized to hold the long value in an unboxed primitive.
     */
    private static abstract class LongNumeratorAbstractCapture extends AbstractCapture {
        private final long numerator;

        public LongNumeratorAbstractCapture(final long nanoTimestamp,
                                            final long numerator) {
            super(nanoTimestamp);
            this.numerator = numerator;
        }


        @Override
        public BigDecimal numerator() {
            return BigDecimal.valueOf(this.numerator);
        }
    }

    /**
     * A partially-optimized implementation of {@link Capture} whose numerator
     * is a primitive long and whose denominator is boxed.
     *
     * @param <DD> the boxed type of the denominator.
     */
    private static class LongNumberCapture<DD extends Number> extends LongNumeratorAbstractCapture {
        private final DD denominator;

        public <NN extends Number> LongNumberCapture(final long nanoTimestamp,
                                                     final NN numerator,
                                                     final DD denominator) {
            super(nanoTimestamp, (long)numerator);
            this.denominator = denominator;
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(this.denominator.doubleValue());
        }
    }

    /**
     * A fully-optimized implementation of {@link Capture} whose numerator
     * and denominator are both primitive longs.
     */
    private static class LongLongCapture extends LongNumeratorAbstractCapture {
        private final long denominator;

        public <NN extends Number, DD extends Number> LongLongCapture(final long nanoTimestamp,
                                                                      final NN numerator,
                                                                      final DD denominator) {
            super(nanoTimestamp, (long)numerator);
            this.denominator = (long) denominator;
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(denominator);
        }
    }

    interface RetentionPolicy {
        long minimumResolutionNanos();
        long maximumRetentionNanos();
        long forceCompactionNanos();
    }

    enum BuiltInRetentionPolicy implements RetentionPolicy {
                             // MAX_RETENTION,  MIN_RESOLUTION             @1sec  @5sec
                CURRENT(Duration.ofSeconds(10), Duration.ofSeconds(1)), //10ish  3ish
          LAST_1_MINUTE(Duration.ofMinutes(1),  Duration.ofSeconds(3)), //20ish  12ish
         LAST_5_MINUTES(Duration.ofMinutes(5),  Duration.ofSeconds(15)),//20ish        -> 31ish. 10s
        LAST_15_MINUTES(Duration.ofMinutes(15), Duration.ofSeconds(30)),//30ish        -> 37ish
            LAST_1_HOUR(Duration.ofHours(1),    Duration.ofMinutes(1)), //60ish
          LAST_24_HOURS(Duration.ofHours(24),   Duration.ofMinutes(15)),//100ish
        ;

        final long minimumResolutionNanos;
        final long maximumRetentionNanos;

        final long forceCompactionNanos;

        final transient String nameLower;

        BuiltInRetentionPolicy(final Duration maximumRetention, final Duration minimumResolution) {
            this.maximumRetentionNanos = maximumRetention.toNanos();
            this.minimumResolutionNanos = minimumResolution.toNanos();

            // we generally rely on query-time compaction, and only perform insertion-time compaction
            // if our series' head entry is significantly older than our maximum retention, which
            // allows us to ensure a reasonable upper-bound of collection size without incurring the
            // cost of compaction too often or inspecting the collection's size.
            final long forceCompactionMargin = Math.max(Duration.ofSeconds(30).toNanos(),
                                                        Math.multiplyExact(minimumResolutionNanos, 8));
            this.forceCompactionNanos = Math.addExact(maximumRetentionNanos, forceCompactionMargin);

            this.nameLower = name().toLowerCase();
        }

        @Override
        public long minimumResolutionNanos() {
            return this.minimumResolutionNanos;
        }

        @Override
        public long maximumRetentionNanos() {
            return this.maximumRetentionNanos;
        }

        @Override
        public long forceCompactionNanos() {
            return this.forceCompactionNanos;
        }

        @Override
        public String toString() {
            return nameLower;
        }
    }

    private class RollingAverageSeries {
        private final AtomicReference<Capture> stage = new AtomicReference<>();
        private final AtomicReference<Node> tail = new AtomicReference<>();

        private final AtomicReference<Node> head = new AtomicReference<>();

        private final RetentionPolicy policy;

        public RollingAverageSeries(final RetentionPolicy policy, final Capture zeroCapture) {
            this.policy = policy;

            final Node zeroNode = new Node(zeroCapture);
            this.head.set(zeroNode);
            this.tail.set(zeroNode);
        }

        private void append(final Capture nextCapture) {
            LOGGER.trace("[{}/{}].append({})", ExtendedFlowMetric.this.getName(), this.policy, nextCapture);
            final Node casTail = this.tail.get(); // for CAS
            // always stage
            final Capture previouslyStaged = stage.getAndSet(nextCapture);

            // promote previously-staged if it is required to maintain our minimum resolution.
            // nextCapture.nanoTime() - lastCommit >= policy.minimumResolutionNanos)
            if (previouslyStaged != null && nextCapture.nanoTime() - casTail.capture.nanoTime() > policy.minimumResolutionNanos()) {
                LOGGER.trace("[{}/{}]//PROMOTE-TRY({})", ExtendedFlowMetric.this.getName(), this.policy, nextCapture);
                final Node proposedNode = new Node(previouslyStaged);
                if (this.tail.compareAndSet(casTail, proposedNode)) {
                    LOGGER.trace("[{}/{}]//PROMOTE-WIN({})", ExtendedFlowMetric.this.getName(), this.policy, nextCapture);
                    casTail.next.set(proposedNode);
                    maybeCompact(nextCapture.nanoTime());
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[{}/{}]//DATA-POINTS-RETAINED({})", ExtendedFlowMetric.this.getName(), this.policy, effectiveSize());
                    }
                } else {
                    LOGGER.trace("[{}/{}]//PROMOTE-NIX({})", ExtendedFlowMetric.this.getName(), this.policy, nextCapture);
                }
            }
        }

        public Optional<Capture> baseline(final long nanoTime) {
            final long threshold = nanoTime - policy.maximumRetentionNanos();
            final Node head = compactUntil(threshold);
            if (head.capture.nanoTime() <= threshold) {
                return Optional.of(head.capture);
            } else {
                return Optional.empty();
            }
        }

        /**
         * @return a computationally-expensive estimate of the number of captures in this series.
         */
        private int effectiveSize() {
            int i = 1; // start at 1 to include our unlinked stage
            // NOTE: while we get our head with volatile-get,  we chase its tail
            // with plain-gets to tolerate missed appends from other threads.
            for (Node current = this.head.get(); current != null; current = current.next.getPlain()) { i++; }
            return i;
        }

        private Node compactUntil(final long threshold) {
            return this.head.updateAndGet((ex) -> Objects.requireNonNullElse(ex.seekWithoutCrossing(threshold), ex));
        }

        private void maybeCompact(final long nanoTime) {
            final long forceCompactionThreshold = nanoTime - policy.forceCompactionNanos();
            if (head.getPlain().capture.nanoTime() < forceCompactionThreshold) {
                if (LOGGER.isDebugEnabled()) {  LOGGER.debug("[{}/{}]//pre-force-compaction-size({})", ExtendedFlowMetric.this.getName(), this.policy, effectiveSize()); }
                compactUntil(nanoTime - policy.maximumRetentionNanos());
                if (LOGGER.isDebugEnabled()) {  LOGGER.debug("[{}/{}]//post-force-compaction-size({})", ExtendedFlowMetric.this.getName(), this.policy, effectiveSize()); }
            }
        }

        private class Node {
            private final AtomicReference<Node> next = new AtomicReference<>();
            private final Capture capture;

            public Node(final Capture capture) {
                this.capture = capture;
            }

            public Node seekWithoutCrossing(final long threshold) {
                Node newestOlderThanThreshold = null;
                Node candidate = this;

                while(candidate != null && candidate.capture.nanoTime() < threshold) {
                    newestOlderThanThreshold = candidate;
                    candidate = candidate.next.get();
                }
                return newestOlderThanThreshold;
            }
        }
    }
}
