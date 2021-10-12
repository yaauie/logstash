# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require "logstash/api/rack_app"
require "puma"
require "puma/server"
require "logstash/patches/puma"
require "concurrent"
require "thread"

module LogStash
  class WebServer

    attr_reader :logger, :config, :http_host, :http_ports, :http_environment, :agent

    DEFAULT_HOST = "127.0.0.1".freeze
    DEFAULT_PORTS = (9600..9700).freeze
    DEFAULT_ENVIRONMENT = 'production'.freeze

    def self.from_settings(logger, agent, settings)
      options = {}
      options[:http_host] = settings.get('api.http.host')
      options[:http_port] = settings.get('api.http.port')
      options[:http_environment] = settings.get('api.environment')

      if settings.get('api.ssl.enabled')
        ssl_params = {}
        ssl_params['keystore'] = settings.get('api.ssl.keystore.path') || fail('Setting `api.ssl.keystore.path` required when `api.ssl.enabled` is true.')
        keystore_pass_wrapper = settings.get('api.ssl.keystore.password') || fail('Setting `api.ssl.keystore.password` required when `api.ssl.enabled` is true.')
        ssl_params['keystore-pass'] = keystore_pass_wrapper.value

        options[:ssl_params] = ssl_params.freeze
      end

      new(logger, agent, options)
    end

    def initialize(logger, agent, options={})
      @logger = logger
      @agent = agent
      @http_host = options[:http_host] || DEFAULT_HOST
      @http_ports = options[:http_ports] || DEFAULT_PORTS
      @http_environment = options[:http_environment] || DEFAULT_ENVIRONMENT
      @ssl_params = options[:ssl_params] if options.include?(:ssl_params)
      @running = Concurrent::AtomicBoolean.new(false)

      # wrap any output that puma could generate into a wrapped logger
      # use the puma namespace to override STDERR, STDOUT in that scope.
      Puma::STDERR.logger = logger
      Puma::STDOUT.logger = logger

      @app = LogStash::Api::RackApp.app(logger, agent, http_environment)
    end

    def run
      logger.debug("Starting puma")

      stop # Just in case

      running!

      bind_to_available_port # and block...

      logger.debug("webserver has stopped running")
    end

    def running!
      @running.make_true
    end

    def running?
      @running.value
    end

    def address
      "#{http_host}:#{@port}"
    end

    def stop(options={})
      @running.make_false
      @server.stop(true) if @server
    end

    private

    def _init_server
      io_wrapped_logger = LogStash::IOWrappedLogger.new(logger)
      events = LogStash::NonCrashingPumaEvents.new(io_wrapped_logger, io_wrapped_logger)

      ::Puma::Server.new(@app, events)
    end

    def bind_to_available_port
      http_ports.each_with_index do |candidate_port, idx|
        begin
          break unless running?

          @server = _init_server

          logger.debug("Trying to start WebServer", :port => candidate_port)
          if @ssl_params
            ssl_context = Puma::MiniSSL::ContextBuilder.new(@ssl_params, @server.events).context
            @server.add_ssl_listener(http_host, candidate_port, ssl_context)
          else
            @server.add_tcp_listener(http_host, candidate_port)
          end

          @port = candidate_port
          logger.info("Successfully started Logstash API endpoint", :port => candidate_port)
          set_http_address_metric("#{http_host}:#{candidate_port}")

          @server.run.join
          break
        rescue Errno::EADDRINUSE
          if http_ports.count == 1
            raise Errno::EADDRINUSE.new(I18n.t("logstash.web_api.cant_bind_to_port", :port => http_ports.first))
          elsif idx == http_ports.count-1
            raise Errno::EADDRINUSE.new(I18n.t("logstash.web_api.cant_bind_to_port_in_range", :http_ports => http_ports))
          end
        end
      end
    end

    def set_http_address_metric(value)
      return unless @agent.metric
      @agent.metric.gauge([], :http_address, value)
    end
  end
end
