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

require 'rubygems'

::Gem.clear_paths

ENV['GEM_HOME'] = ENV['GEM_PATH'] = ::File.expand_path(
    ::File.join(__FILE__, "..", "..", "..", "build", "qa", "integration", "vendor", "jruby", "3.1.0")
)

::Gem.paths = ENV

require "bundler"
::Bundler.setup

require "rspec"
require "rspec/core"

RSpec.clear_examples

class ExampleTimer
  TIMINGS_FILE=File.expand_path("timings.tsv", __dir__)

  def initialize
    @timings = Hash.new(0)
  end

  def record(example)
    start_time = Time.now
    example.run
  ensure
    @timings[Pathname.new(example.file_path).cleanpath.to_s] += (Time.now - start_time)
  end

  def write
    @timings.each do |filename, time_seconds|
      $stderr.puts("[TIME] #{filename} (actual: #{time_seconds.ceil})")
    end
  end
end
timer = ExampleTimer.new

RSpec.configure do |configuration|
  configuration.around(:context) do |example_group|
    timer.record(example_group)
  end
  configuration.after(:suite) do
    timer.write
  end
end

return RSpec::Core::Runner.run($JUNIT_ARGV).to_i
