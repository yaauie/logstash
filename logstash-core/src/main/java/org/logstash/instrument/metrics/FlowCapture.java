package org.logstash.instrument.metrics;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * A {@link FlowCapture} is used by {@link FlowMetric} to hold
 * point-in-time data for a pair of {@link Metric}s. It includes
 * a variety of implementations, all of which are immutable.
 *
 * <p>Users of this interface are not expected to instantiate
 * the implementations directly, but instead should use a
 * {@link FlowCapture.Factory}, which can be obtained through
 * either {@link FlowCapture#factoryFor(Metric, Metric)} or
 * {@link FlowCapture#unoptimizedFactory()}.
 */
interface FlowCapture {
    /**
     * @return the nanoTime of this capture, as provided at time
     *         of capture by the {@link FlowMetric}.
     */
    long nanoTime();

    /**
     * @return the value of the numerator metric at time of capture.
     */
    BigDecimal numerator();

    /**
     * @return the value of the denominator metric at time of capture.
     */
    BigDecimal denominator();

    /**
     * The {@link Factory} is the intended way to create instances
     * of {@link FlowCapture}, and one can be acquired through
     *   {@link FlowCapture#factoryFor(Metric, Metric)} or
     *   {@link FlowCapture#unoptimizedFactory()}.
     */
    @FunctionalInterface
    interface Factory {
        FlowCapture createCapture(final long nanoTime,
                                  final Number numeratorValue,
                                  final Number denominatorValue);
    }

    /**
     * @param numeratorMetric the numerator metric
     * @param denominatorMetric the denominator metric
     * @return the optimal {@link Factory} for creating {@link FlowCapture}s
     *         of the values produced by the provided metrics.
     */
    static Factory factoryFor(final Metric<? extends Number> numeratorMetric,
                              final Metric<? extends Number> denominatorMetric) {
        Objects.requireNonNull(numeratorMetric, "numeratorMetric");
        Objects.requireNonNull(denominatorMetric, "denominatorMetric");

        switch (numeratorMetric.getType()) {
            case COUNTER_LONG:
                switch(denominatorMetric.getType()) {
                    case COUNTER_LONG:    return LongLongFlowCapture::new;
                    case COUNTER_DECIMAL: return LongDoubleFlowCapture::new;
                    default:              return LongNumeratorFlowCapture::new;
                }
            case COUNTER_DECIMAL:
                switch(denominatorMetric.getType()) {
                    case COUNTER_DECIMAL: return DoubleDoubleFlowCapture::new;
                    case COUNTER_LONG:    return DoubleLongFlowCapture::new;
                    default:              return DoubleNumeratorFlowCapture::new;
                }
            default:
                switch(denominatorMetric.getType()) {
                    case COUNTER_LONG:    return LongDenominatorFlowCapture::new;
                    case COUNTER_DECIMAL: return DoubleDenominatorFlowCapture::new;
                    default:              return unoptimizedFactory();
                }
        }
    }

    /**
     * @return an unoptimized {@link Factory} for creating {@link FlowCapture}s.
     */
    static Factory unoptimizedFactory() {
        return UnoptimizedFlowCapture::new;
    }

    /**
     * The {@link AbstractBaseFlowCapture} is the base implementation of
     * all internal {@link FlowCapture}s.
     *
     * <p>These internal implementations follow the below naming pattern:
     * <ul>
     *   <li>{@code UnoptimizedFlowCapture}: not optimized (boxed numerator and denominator)</li>
     *   <li>{@code [numeratorType]NumeratorFlowCapture}: partially optimized (boxed denominator)</li>
     *   <li>{@code [denominatorType]DenominatorFlowCapture}: partially optimized (boxed numerator)</li>
     *   <li>{@code [{numeratorType][denominatorType]FlowCapture}: fully-optimized</li>
     * </ul>
     */
    abstract class AbstractBaseFlowCapture implements FlowCapture {
        private final long nanoTime;

        public AbstractBaseFlowCapture(long nanoTime) {
            this.nanoTime = nanoTime;
        }

        @Override
        public long nanoTime() {
            return nanoTime;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() +"{" +
                    "nanoTimestamp=" + nanoTime +
                    " numerator=" + numerator() +
                    " denominator=" + denominator() +
                    '}';
        }
    }

    abstract class AbstractBoxedNumeratorFlowCapture extends AbstractBaseFlowCapture {
        private final Number numerator;

        public AbstractBoxedNumeratorFlowCapture(long nanoTimestamp,
                                                 Number numerator) {
            super(nanoTimestamp);
            this.numerator = numerator;
        }

        @Override
        public BigDecimal numerator() {
            return BigDecimal.valueOf(numerator.doubleValue());
        }
    }

    class LongDenominatorFlowCapture extends AbstractBoxedNumeratorFlowCapture {
        final long denominator;

        public LongDenominatorFlowCapture(final long nanoTimestamp,
                                          final Number numerator,
                                          final Number denominator) {
            super(nanoTimestamp, numerator);
            this.denominator = denominator.longValue();
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(denominator);
        }
    }

    class DoubleDenominatorFlowCapture extends AbstractBoxedNumeratorFlowCapture {
        final double denominator;

        public DoubleDenominatorFlowCapture(final long nanoTimestamp,
                                            final Number numerator,
                                            final Number denominator) {
            super(nanoTimestamp, numerator);
            this.denominator = denominator.doubleValue();
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(denominator);
        }
    }

    /**
     * An unoptimized, fully-boxed implementation of {@link FlowCapture}.
     * This implementation uses the most memory per capture.
     */
    class UnoptimizedFlowCapture extends AbstractBoxedNumeratorFlowCapture {
        final Number denominator;

        UnoptimizedFlowCapture(final long nanoTimestamp,
                               final Number numerator,
                               final Number denominator) {
            super(nanoTimestamp, numerator);
            this.denominator = denominator;
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(denominator.doubleValue());
        }
    }

    /**
     * A common abstract class for {@link FlowCapture}s whose numerator is a long,
     * optimized to hold the long value in an unboxed primitive.
     */
    abstract class AbstractLongNumeratorFlowCapture extends AbstractBaseFlowCapture {
        private final long numerator;

        public AbstractLongNumeratorFlowCapture(final long nanoTimestamp,
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
     * A partially-optimized implementation of {@link FlowCapture} whose numerator
     * is a primitive long and whose denominator is boxed.
     */
    class LongNumeratorFlowCapture extends AbstractLongNumeratorFlowCapture {
        private final Number denominator;

        public LongNumeratorFlowCapture(final long nanoTimestamp,
                                        final Number numerator,
                                        final Number denominator) {
            super(nanoTimestamp, numerator.longValue());
            this.denominator = denominator;
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(this.denominator.doubleValue());
        }
    }

    /**
     * A fully-optimized implementation of {@link FlowCapture} whose numerator
     * and denominator are both primitive longs.
     */
    class LongLongFlowCapture extends AbstractLongNumeratorFlowCapture {
        private final long denominator;

        public LongLongFlowCapture(final long nanoTimestamp,
                                   final Number numerator,
                                   final Number denominator) {
            super(nanoTimestamp, numerator.longValue());
            this.denominator = denominator.longValue();
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(denominator);
        }
    }

    /**
     * A fully-optimized implementation of {@link FlowCapture} whose numerator
     * and denominator are primitive long and double respectively.
     */
    class LongDoubleFlowCapture extends AbstractLongNumeratorFlowCapture {
        private final double denominator;

        public LongDoubleFlowCapture(final long nanoTimestamp,
                                     final Number numerator,
                                     final Number denominator) {
            super(nanoTimestamp, numerator.longValue());
            this.denominator = denominator.doubleValue();
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(denominator);
        }
    }

    /**
     * A common abstract class for {@link FlowCapture}s whose numerator is a double,
     * optimized to hold the long value in an unboxed primitive.
     */
    abstract class AbstractDoubleNumeratorFlowCapture extends AbstractBaseFlowCapture {
        private final double numerator;

        public AbstractDoubleNumeratorFlowCapture(final long nanoTimestamp,
                                                  final double numerator) {
            super(nanoTimestamp);
            this.numerator = numerator;
        }

        @Override
        public BigDecimal numerator() {
            return BigDecimal.valueOf(this.numerator);
        }
    }

    /**
     * A partially-optimized implementation of {@link FlowCapture} whose numerator
     * is a primitive long and whose denominator is boxed.
     */
    class DoubleNumeratorFlowCapture extends AbstractDoubleNumeratorFlowCapture {
        private final Number denominator;

        public DoubleNumeratorFlowCapture(final long nanoTimestamp,
                                          final Number numerator,
                                          final Number denominator) {
            super(nanoTimestamp, numerator.doubleValue());
            this.denominator = denominator;
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(this.denominator.doubleValue());
        }
    }

    /**
     * A fully-optimized implementation of {@link FlowCapture} whose numerator
     * and denominator are primitive double and long respectively.
     */
    class DoubleLongFlowCapture extends AbstractDoubleNumeratorFlowCapture {
        private final long denominator;

        DoubleLongFlowCapture(final long nanoTimestamp,
                              final Number numerator,
                              final Number denominator) {
            super(nanoTimestamp, numerator.doubleValue());
            this.denominator = denominator.longValue();
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(denominator);
        }
    }

    /**
     * A fully-optimized implementation of {@link FlowCapture} whose numerator
     * and denominator are both primitive doubles.
     */
    class DoubleDoubleFlowCapture extends AbstractDoubleNumeratorFlowCapture {
        private final double denominator;

        public DoubleDoubleFlowCapture(final long nanoTimestamp,
                                       final Number numerator,
                                       final Number denominator) {
            super(nanoTimestamp, numerator.doubleValue());
            this.denominator = denominator.doubleValue();
        }

        @Override
        public BigDecimal denominator() {
            return BigDecimal.valueOf(denominator);
        }
    }
}
