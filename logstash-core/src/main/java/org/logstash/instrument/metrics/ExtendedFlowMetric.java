package org.logstash.instrument.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ExtendedFlowMetric<N extends Number,D extends Number> extends AbstractMetric<Map<String,Double>> implements FlowMetric {

    private static final Logger LOGGER = LogManager.getLogger(ExtendedFlowMetric.class);
    static final MathContext THREE_DIGIT_PRECISION = new MathContext(3, RoundingMode.HALF_UP);

    private final LongSupplier nanoTimeSupplier;
    private final Metric<N> numeratorMetric;
    private final Metric<D> denominatorMetric;

    private final Capture baseline;

    private final List<Series> seriesList;

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

        this.baseline = doCapture();

        this.seriesList = Arrays.stream(Policy.values())
                .sequential()
                .map(policy -> new Series(policy, this.baseline))
                .collect(Collectors.toList());
    }

    private Capture doCapture() {
        return new Capture(nanoTimeSupplier.getAsLong(), numeratorMetric.getValue(), denominatorMetric.getValue());
    }

    @Override
    public void capture() {
        final Capture candidateCapture = doCapture();
        this.seriesList.forEach(series -> series.append(candidateCapture));
    }

    @Override
    public Map<String, Double> getValue() {
        final Capture currentCapture = doCapture();

        final Map<String, Double> rates = new HashMap<>();

        this.seriesList.forEach(series -> {
            final Optional<Capture> seriesBaseline = series.baseline(currentCapture.nanoTimestamp);
            currentCapture.calculateRate(() -> seriesBaseline.orElse(null))
                          .ifPresent((rate) -> rates.put(series.policy.nameLower, rate));
        });

        currentCapture.calculateRate(baseline).ifPresent((rate) -> rates.put("lifetime", rate));

        return Map.copyOf(rates);
    }

    // a capture MAY have a next (newer)
    // our wrapper has a reference to the _newest_ capture

    // we make an unlinked capture
    // we atomically get the newest, and atomically append our unlinked capture to it IFF it doesn't have one.
    // we rotate our newest forward to its next

    private class Capture {
        private final long nanoTimestamp;

        private final N numerator;
        private final D denominator;

        Capture(final long nanoTimestamp, final N numerator, final D denominator) {
            this.nanoTimestamp = nanoTimestamp;
            this.numerator = numerator;
            this.denominator = denominator;
        }

        OptionalDouble calculateRate(final Capture baseline) {
            Objects.requireNonNull(baseline, "baseline");
            if (baseline == this) { return OptionalDouble.empty(); }

            final double deltaNumerator = this.numerator.doubleValue() - baseline.numerator.doubleValue();
            final double deltaDenominator = this.denominator.doubleValue() - baseline.denominator.doubleValue();

            // divide-by-zero safeguard
            if (deltaDenominator == 0.0) { return OptionalDouble.empty(); }

            // To prevent the appearance of false-precision, we round to 3 decimal places.
            return OptionalDouble.of(BigDecimal.valueOf(deltaNumerator)
                                               .divide(BigDecimal.valueOf(deltaDenominator), 4, RoundingMode.FLOOR)
                                               .round(THREE_DIGIT_PRECISION)
                                               .doubleValue());
        }

        OptionalDouble calculateRate(final Supplier<Capture> possibleBaseline) {
            return Optional.ofNullable(possibleBaseline.get())
                    .map(this::calculateRate)
                    .orElseGet(OptionalDouble::empty);
        }

        @Override
        public String toString() {
            return "Capture{" +
                    "nanoTimestamp=" + nanoTimestamp +
                    ", numerator=" + numerator +
                    ", denominator=" + denominator +
                    '}';
        }
    }

    enum Policy {
        CURRENT(Duration.ofSeconds(10), Duration.ofSeconds(1)),
        LAST_1_MINUTE(Duration.ofMinutes(1), Duration.ofSeconds(3)),
        LAST_3_MINUTES(Duration.ofMinutes(3), Duration.ofSeconds(10)),
        LAST_5_MINUTES(Duration.ofMinutes(5), Duration.ofSeconds(15)),
        LAST_15_MINUTES(Duration.ofMinutes(15), Duration.ofSeconds(30)),
        LAST_1_HOUR(Duration.ofHours(1), Duration.ofMinutes(1)),
        ;

        final long minimumResolutionNanos;
        final long maximumRetentionNanos;

        final transient String nameLower;

        Policy(final Duration maximumRetention, final Duration minimumResolution) {
            this.maximumRetentionNanos = maximumRetention.toNanos();
            this.minimumResolutionNanos = minimumResolution.toNanos();

            this.nameLower = name().toLowerCase();
        }
    }

    private class Series {
        private final AtomicReference<Capture> staging = new AtomicReference<>();
        private final ConcurrentLinkedQueue<Capture> committedCaptures;
        private final AtomicLong lastCommitted;

        private final Policy policy;

        public Series(final Policy policy, final Capture zeroCapture) {
            this.policy = policy;

            this.committedCaptures = new ConcurrentLinkedQueue<>(Collections.singleton(zeroCapture));
            this.lastCommitted = new AtomicLong(zeroCapture.nanoTimestamp);
        }

        private long datapointCount() {
            return committedCaptures.stream().count();
        }

        void append(final Capture nextCapture) {
            LOGGER.trace("[{}/{}].append({})", ExtendedFlowMetric.this.getName(), this.policy.name(), nextCapture);
            // always promote to first stage
            final long lastCommit = lastCommitted.get();
            final Capture previouslyStaged = staging.getAndSet(nextCapture);

            // promote the previously-staged value IFF the gap between
            // our lastCommit and nextCapture is bigger than our policy minimum resolution
            // TODO: consider not promoting if our denominator has not changed.
            if (previouslyStaged != null && nextCapture.nanoTimestamp - lastCommit >= policy.minimumResolutionNanos) {
                LOGGER.trace("[{}/{}]//PROMOTE-TRY({})", ExtendedFlowMetric.this.getName(), this.policy.name(), nextCapture);
                if (lastCommitted.compareAndSet(lastCommit, previouslyStaged.nanoTimestamp)) {
                    LOGGER.trace("[{}/{}]//PROMOTE-WIN({})", ExtendedFlowMetric.this.getName(), this.policy.name(), nextCapture);
                    committedCaptures.offer(previouslyStaged);
                    // TODO: compact a little less frequently.
                    compactAsOf(nextCapture.nanoTimestamp);
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[{}/{}]//DATA-POINTS-RETAINED({})", ExtendedFlowMetric.this.getName(), this.policy.name(), datapointCount());
                    }
                } else {
                    LOGGER.trace("[{}/{}]//PROMOTE-NIX({})", ExtendedFlowMetric.this.getName(), this.policy.name(), nextCapture);
                }
            }
        }

        public Optional<Capture> baseline() {
            return baseline(nanoTimeSupplier.getAsLong());
        }

        public Optional<Capture> baseline(final long nanoTime) {
            return Optional.ofNullable(compactAsOf(nanoTime));
        }

        private Capture compactAsOf(final long nanoTime) {
            return compactUntil(nanoTime - policy.maximumRetentionNanos);
        }

        private Capture compactUntil(final long nanoTimeThreshold) {
            LOGGER.trace("[{}/{}]//compactUntil({})", ExtendedFlowMetric.this.getName(), this.policy.name(), nanoTimeThreshold);
            final Iterator<Capture> captureIterator = committedCaptures.iterator();

            Capture newestOlderThanThreshold = null;
            Capture candidate;

            while(captureIterator.hasNext()) {
                candidate = captureIterator.next();
                LOGGER.trace("[{}/{}]//compact-candidate({})", ExtendedFlowMetric.this.getName(), this.policy.name(), candidate);
                if (candidate.nanoTimestamp > nanoTimeThreshold) { break; }
                if (newestOlderThanThreshold != null) {
                    LOGGER.trace("[{}/{}]//reap-older({})", ExtendedFlowMetric.this.getName(), this.policy.name(), newestOlderThanThreshold);
                    committedCaptures.remove(newestOlderThanThreshold);
                }
                newestOlderThanThreshold = candidate;
            }

            LOGGER.trace("[{}/{}]//newest-older-than-threshold({})", ExtendedFlowMetric.this.getName(), this.policy.name(), newestOlderThanThreshold);
            return newestOlderThanThreshold;
        }
    }
}
