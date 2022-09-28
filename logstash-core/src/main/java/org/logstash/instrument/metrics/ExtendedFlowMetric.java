package org.logstash.instrument.metrics;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class ExtendedFlowMetric extends BaseFlowMetric {

    private final List<Series> trackedSeries;

    public ExtendedFlowMetric(final String name,
                              final Metric<? extends Number> numeratorMetric,
                              final Metric<? extends Number> denominatorMetric) {
        this(System::nanoTime, name, numeratorMetric, denominatorMetric);
    }

    ExtendedFlowMetric(final LongSupplier nanoTimeSupplier,
                       final String name,
                       final Metric<? extends Number> numeratorMetric,
                       final Metric<? extends Number> denominatorMetric) {
        super(nanoTimeSupplier, name, numeratorMetric, denominatorMetric);

        trackedSeries = Arrays.stream(BuiltInRetentionPolicy.values())
                              .map(this::retentionSeriesFromRetentionPolicy)
                              .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void capture() {
        final FlowCapture candidateCapture = doCapture();
        this.trackedSeries.forEach(series -> series.append(candidateCapture));
    }

    @Override
    public Map<String, Double> getValue() {
        final FlowCapture currentCapture = doCapture();

        final Map<String, Double> rates = new LinkedHashMap<>();

        this.trackedSeries.forEach(series -> series.baseline(currentCapture.nanoTime())
                .map((b) -> calculateRate(currentCapture, b))
                .orElseGet(OptionalDouble::empty)
                .ifPresent((rate) -> rates.put(series.policy.toString(), rate)));

        calculateRate(currentCapture, lifetimeBaseline).ifPresent((rate) -> rates.put("lifetime", rate));

        return Collections.unmodifiableMap(rates);
    }

    private Series retentionSeriesFromRetentionPolicy(BuiltInRetentionPolicy builtInRetentionPolicy) {
        return new Series(builtInRetentionPolicy, this.lifetimeBaseline);
    }

    interface RetentionPolicy {
        long minimumResolutionNanos();
        long maximumRetentionNanos();
        long forceCompactionNanos();
    }

    enum BuiltInRetentionPolicy implements RetentionPolicy {
                              // MAX_RETENTION,  MIN_RESOLUTION           @1sec  @5sec
                CURRENT(Duration.ofSeconds(10), Duration.ofSeconds(1)), //10ish  3ish
          LAST_1_MINUTE(Duration.ofMinutes(1),  Duration.ofSeconds(3)), //20ish  12ish
         LAST_5_MINUTES(Duration.ofMinutes(5),  Duration.ofSeconds(15)),//20ish  -> 31ish ~10s
        LAST_15_MINUTES(Duration.ofMinutes(15), Duration.ofSeconds(30)),//30ish  -> 37ish ~25s
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

    private static class Series {
        private final AtomicReference<FlowCapture> stagedCapture = new AtomicReference<>();
        private final AtomicReference<Node> tail;
        private final AtomicReference<Node> head;
        private final RetentionPolicy policy;

        Series(final RetentionPolicy policy, final FlowCapture zeroCapture) {
            this.policy = policy;
            final Node zeroNode = new Node(zeroCapture);
            this.head = new AtomicReference<>(zeroNode);
            this.tail = new AtomicReference<>(zeroNode);
        }

        private void append(final FlowCapture nextCapture) {
            final Node casTail = this.tail.get(); // for CAS
            // always stage
            final FlowCapture previouslyStaged = stagedCapture.getAndSet(nextCapture);

            // promote previously-staged if it is required to maintain our minimum resolution.
            // nextCapture.nanoTime() - lastCommit >= policy.minimumResolutionNanos)
            if (previouslyStaged != null && nextCapture.nanoTime() - casTail.capture.nanoTime() > policy.minimumResolutionNanos()) {
                final Node proposedNode = new Node(previouslyStaged);
                if (this.tail.compareAndSet(casTail, proposedNode)) {
                    casTail.next.set(proposedNode);
                    maybeCompact(nextCapture.nanoTime());
                }
            }
        }

        public Optional<FlowCapture> baseline(final long nanoTime) {
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
        int effectiveSize() {
            int i = 1; // start at 1 to include our unlinked stage
            // NOTE: while we get our head with volatile-get, we chase its tail
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
                compactUntil(nanoTime - policy.maximumRetentionNanos());
            }
        }

        private static class Node {
            private final FlowCapture capture;
            private final AtomicReference<Node> next = new AtomicReference<>();

            public Node(final FlowCapture capture) {
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
