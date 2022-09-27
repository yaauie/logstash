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

public class ExtendedFlowMetric<N extends Number,D extends Number> extends AbstractMetric<Map<String,Double>> implements FlowMetric {

    private static final Logger LOGGER = LogManager.getLogger(ExtendedFlowMetric.class);


    private final Metric<N> numeratorMetric;
    private final Metric<D> denominatorMetric;

    // @see
    private final CaptureFactory<N,D> captureFactory;

    private final Capture baseline;

    private final List<Series> seriesList;

    // test dependency injection
    private final LongSupplier nanoTimeSupplier;

    // in order to prevent presenting false-precision, we limit the precision to four digits.
    static final MathContext FOUR_DIGIT_PRECISION = new MathContext(4, RoundingMode.HALF_UP);

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
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.numeratorMetric = numeratorMetric;
        this.denominatorMetric = denominatorMetric;

        this.captureFactory = initCaptureFactory();

        this.baseline = doCapture();

        this.seriesList = Arrays.stream(Policy.values())
                .sequential()
                .map(policy -> new Series(policy, this.baseline))
                .collect(Collectors.toList());
    }

    private CaptureFactory<N,D> initCaptureFactory() {
        Objects.requireNonNull(numeratorMetric, "numeratorMetric");
        Objects.requireNonNull(denominatorMetric, "denominatorMetric");

        if (numeratorMetric.getType() == MetricType.COUNTER_LONG) {
            if (denominatorMetric.getType() == MetricType.COUNTER_LONG) {
                LOGGER.debug("{} -> LongLongCapture ({}/{})", getName(), numeratorMetric.getType(), denominatorMetric.getType());
                return LongLongCapture::new;
             }
            LOGGER.debug("{} -> LongNumberCapture ({}/{})", getName(), numeratorMetric.getType(), denominatorMetric.getType());
            return LongNumberCapture::new;
        }
        LOGGER.debug("{} -> NumberNumberCapture ({}/{})", getName(), numeratorMetric.getType(), denominatorMetric.getType());
        return NumberNumberCapture::new;
    }

    @FunctionalInterface
    interface CaptureFactory<NN extends Number, DD extends Number> {
        Capture create(final long nanoTimestamp, final NN numerator, final DD denominator);
    }

    private Capture doCapture() {
        return captureFactory.create(nanoTimeSupplier.getAsLong(), numeratorMetric.getValue(), denominatorMetric.getValue());
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
                                                .ifPresent((rate) -> rates.put(series.policy.nameLower, rate)));

        getRate(currentCapture, baseline).ifPresent((rate) -> rates.put("lifetime", rate));

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

    private static abstract class Capture {
        private final long nanoTimestamp;

        public Capture(long nanoTimestamp) {
            this.nanoTimestamp = nanoTimestamp;
        }

        public long nanoTime() {
            return nanoTimestamp;
        }

        abstract BigDecimal numerator();
        abstract BigDecimal denominator();
    }

    private class NumberNumberCapture extends Capture {

        private final N numerator;
        private final D denominator;

        NumberNumberCapture(final long nanoTimestamp,
                            final N numerator,
                            final D denominator) {
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

    private abstract class LongNumeratorAbstractCapture extends Capture {
        private final long numerator;

        public LongNumeratorAbstractCapture(final long nanoTimestamp,
                                            final N numerator) {
            super(nanoTimestamp);
            this.numerator = (long) numerator;
        }


        @Override
        public BigDecimal numerator() {
            return BigDecimal.valueOf(this.numerator);
        }
    }

    private class LongNumberCapture extends LongNumeratorAbstractCapture {
        private final D denominator;

        public LongNumberCapture(final long nanoTimestamp,
                                 final N numerator,
                                 D denominator) {
            super(nanoTimestamp, numerator);
            this.denominator = denominator;
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(this.denominator.doubleValue());
        }
    }

    private class LongLongCapture extends LongNumeratorAbstractCapture {
        private final long denominator;

        public LongLongCapture(final long nanoTimestamp,
                               final N numerator,
                               final D denominator) {
            super(nanoTimestamp, numerator);
            this.denominator = (long) denominator;
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(denominator);
        }
    }

    enum Policy {
                             // MAX_RETENTION, MIN_RESOLUTION             @1sec  @5sec
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

        Policy(final Duration maximumRetention, final Duration minimumResolution) {
            this.maximumRetentionNanos = maximumRetention.toNanos();
            this.minimumResolutionNanos = minimumResolution.toNanos();

            // we force compaction on insertion only if the oldest entry is substantially too old,
            // enough for roughly the greater of either 30 seconds or roughly 8 extra data-points
            // per the configured minimum resolution.
            // This reduces the insertion-time work significantly
            // while still ensuring that our collections don't grow unbounded.
            final long forceCompactionMargin = Math.max(Duration.ofSeconds(30).toNanos(), Math.multiplyExact(minimumResolutionNanos, 8));
            this.forceCompactionNanos = Math.addExact(maximumRetentionNanos, forceCompactionMargin);

            this.nameLower = name().toLowerCase();
        }
    }

    private class Series {
        private final AtomicReference<Capture> stage = new AtomicReference<>();
        private final AtomicReference<Node> tail = new AtomicReference<>();

        private final AtomicReference<Node> head = new AtomicReference<>();

        private final Policy policy;

        public Series(final Policy policy, final Capture zeroCapture) {
            this.policy = policy;

            final Node zeroNode = new Node(zeroCapture);
            this.head.set(zeroNode);
            this.tail.set(zeroNode);
        }

        private void append(final Capture nextCapture) {
            LOGGER.trace("[{}/{}].append({})", ExtendedFlowMetric.this.getName(), this.policy.name(), nextCapture);
            final Node casTail = this.tail.get(); // for CAS
            // always stage
            final Capture previouslyStaged = stage.getAndSet(nextCapture);

            // promote previously-staged if it is required to maintain our minimum resolution.
            // nextCapture.nanoTime() - lastCommit >= policy.minimumResolutionNanos)
            if (previouslyStaged != null && nextCapture.nanoTime() - casTail.capture.nanoTime() > policy.minimumResolutionNanos) {
                LOGGER.trace("[{}/{}]//PROMOTE-TRY({})", ExtendedFlowMetric.this.getName(), this.policy.name(), nextCapture);
                final Node proposedNode = new Node(previouslyStaged);
                if (this.tail.compareAndSet(casTail, proposedNode)) {
                    LOGGER.trace("[{}/{}]//PROMOTE-WIN({})", ExtendedFlowMetric.this.getName(), this.policy.name(), nextCapture);
                    casTail.next.set(proposedNode);
                    maybeCompact(nextCapture.nanoTime());
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[{}/{}]//DATA-POINTS-RETAINED({})", ExtendedFlowMetric.this.getName(), this.policy.name(), effectiveSize() + 1);
                    }
                } else {
                    LOGGER.trace("[{}/{}]//PROMOTE-NIX({})", ExtendedFlowMetric.this.getName(), this.policy.name(), nextCapture);
                }
            }
        }

        public Optional<Capture> baseline(final long nanoTime) {
            final long threshold = nanoTime - policy.maximumRetentionNanos;
            final Node head = compactUntil(threshold);
            if (head.capture.nanoTime() <= threshold) {
                return Optional.of(head.capture);
            } else {
                return Optional.empty();
            }
        }

        int effectiveSize() {
            int i = 1;
            for (Node current = this.head.getPlain(); current != null; current = current.next.getPlain()) { i++; }
            return i;
        }

        private Node compactUntil(final long threshold) {
            return this.head.updateAndGet((ex) -> Objects.requireNonNullElse(ex.seekWithoutCrossing(threshold), ex));
        }

        private void maybeCompact(final long nanoTime) {
            final long forceCompactionThreshold = nanoTime - policy.forceCompactionNanos;
            if (head.getPlain().capture.nanoTime() < forceCompactionThreshold) {
                if (LOGGER.isDebugEnabled()) {  LOGGER.debug("[{}/{}]//pre-force-compaction-size({})", ExtendedFlowMetric.this.getName(), this.policy.name(), effectiveSize()); }
                compactUntil(nanoTime - policy.maximumRetentionNanos);
                if (LOGGER.isDebugEnabled()) {  LOGGER.debug("[{}/{}]//post-force-compaction-size({})", ExtendedFlowMetric.this.getName(), this.policy.name(), effectiveSize()); }
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
