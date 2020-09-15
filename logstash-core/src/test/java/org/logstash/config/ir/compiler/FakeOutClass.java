/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.logstash.config.ir.compiler;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.concurrent.atomic.AtomicReference;

import static org.logstash.RubyUtil.RUBY;

@SuppressWarnings("serial")
@JRubyClass(name = "FakeOutClass")
public class FakeOutClass extends RubyObject {
    @JRubyClass(name = "InstanceFactory")
    public static class InstanceFactory extends RubyObject {
        private IRubyObject pluginKlass;
        private IRubyObject executionContext;
        private IRubyObject metric;

        InstanceFactory(Ruby runtime, RubyClass metaClass) {
            super(runtime, metaClass);
        }

        @JRubyMethod
        public IRubyObject initialize(final ThreadContext context, final IRubyObject pluginKlass, final IRubyObject executionContext, final IRubyObject metric) {
            this.pluginKlass = pluginKlass;
            this.executionContext = executionContext;
            this.metric = metric;

            return this;
        }

        @JRubyMethod(rest = true)
        public IRubyObject create(final ThreadContext context, final IRubyObject[] args) {
            InstanceFactory previous = null;
            try {
                previous = currentInstanceFactory.getAndSet(this);
                return pluginKlass.callMethod(context, "new", args);
            } finally {
                currentInstanceFactory.set(previous);
            }
        }
    }

    private static AtomicReference<InstanceFactory> currentInstanceFactory = new AtomicReference<>();
    public static FakeOutClass latestInstance;

    private static IRubyObject OUT_STRATEGY = RUBY.newSymbol("single");

    private int multiReceiveDelay = 0;
    private int multiReceiveCallCount = 0;
    private int registerCallCount = 0;
    private int closeCallCount = 0;
    private IRubyObject multiReceiveArgs;
    private IRubyObject metricArgs;

    FakeOutClass(final Ruby runtime, final RubyClass metaClass) {
        super(runtime, metaClass);
    }

    static FakeOutClass create() {
        return new FakeOutClass(RUBY, OutputDelegatorTest.FAKE_OUT_CLASS);
    }

    @JRubyMethod
    public IRubyObject name(final ThreadContext context) {
        return RUBY.newString("example");
    }

    @JRubyMethod(name = "config_name", meta = true)
    public static IRubyObject configName(final ThreadContext context, final IRubyObject recv) {
        return RUBY.newString("dummy_plugin");
    }

    @JRubyMethod
    public IRubyObject initialize(final ThreadContext context, final IRubyObject args) {
//        // TODO: replicate init behaviour being sucked into plugin instance factory o_O
        final InstanceFactory instanceFactory = currentInstanceFactory.get();
        if (instanceFactory != null) {
            this.metric(instanceFactory.metric);
            this.executionContext(instanceFactory.executionContext);
        }
        latestInstance = this;
        return this;
    }

    @JRubyMethod(meta = true)
    public static IRubyObject concurrency(final ThreadContext context, final IRubyObject recv) {
        return OUT_STRATEGY;
    }

    @JRubyMethod
    public IRubyObject register() {
        registerCallCount++;
        return this;
    }

    // TODO: this is super janky replication of core logic for tests O_o
    @JRubyMethod(name = "instance_factory", meta = true, required = 2)
    public static IRubyObject instanceFactory(final ThreadContext context, final IRubyObject recv, final IRubyObject executionContext, final IRubyObject metric) {
        return OutputDelegatorTest.FAKE_OUT_INSTANCE_FACTORY_CLASS.callMethod(context, "new", new IRubyObject[]{recv, executionContext, metric});
    }

    @JRubyMethod(name = "metric=")
    public IRubyObject metric(final IRubyObject args) {
        this.metricArgs = args;
        return this;
    }

    @JRubyMethod(name = "execution_context=")
    public IRubyObject executionContext(IRubyObject args) {
        return this;
    }

    @JRubyMethod(name = AbstractOutputDelegatorExt.OUTPUT_METHOD_NAME)
    public IRubyObject multiReceive(final IRubyObject args) {
        multiReceiveCallCount++;
        multiReceiveArgs = args;
        if (multiReceiveDelay > 0) {
            try {
                Thread.sleep(multiReceiveDelay);
            } catch (InterruptedException e) {
                // do nothing
            }
        }
        return this;
    }

    @JRubyMethod(name = "do_close")
    public IRubyObject close(final ThreadContext context) {
        closeCallCount++;
        return this;
    }

    @JRubyMethod(name = "set_out_strategy")
    public static IRubyObject setOutStrategy(final ThreadContext context, final IRubyObject recv,
        final IRubyObject outStrategy) {
        OUT_STRATEGY = outStrategy;
        return context.nil;
    }

    public IRubyObject getMetricArgs() {
        return this.metricArgs;
    }

    public IRubyObject getMultiReceiveArgs() {
        return multiReceiveArgs;
    }

    public int getMultiReceiveCallCount() {
        return this.multiReceiveCallCount;
    }

    public void setMultiReceiveDelay(int delay) {
        this.multiReceiveDelay = delay;
    }

    public int getRegisterCallCount() {
        return registerCallCount;
    }

    public int getCloseCallCount() {
        return closeCallCount;
    }
}
