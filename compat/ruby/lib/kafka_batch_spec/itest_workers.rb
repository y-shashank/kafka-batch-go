# frozen_string_literal: true

# Integration-test workers for cross-runtime matrix specs.
# Loaded before the handler manifest so worker_class constants resolve.

module KafkaBatchSpec
  module ItestWorkers
    module_function

    def marker_path
      ENV.fetch("KBATCH_RUBY_ITEST_MARKER", "")
    end

    def write_marker!(content)
      path = marker_path
      return if path.empty?

      File.write(path, content.to_s)
    end
  end
end

class RubyPlainWorker
  include KafkaBatch::Worker

  job_type "integration.ruby_plain"

  def perform(_payload)
    KafkaBatchSpec::ItestWorkers.write_marker!(job_id)
  end
end

class RubyFairWorker
  include KafkaBatch::Worker

  job_type "integration.ruby_fair"
  fairness_type :time

  def perform(payload)
    tenant = payload["tenant"] || payload[:tenant]
    KafkaBatchSpec::ItestWorkers.write_marker!("#{job_id}:#{tenant}")
  end
end

class RubyRetryOnceWorker
  include KafkaBatch::Worker

  job_type "integration.ruby_retry_once"
  max_retries 2

  def perform(_payload)
    raise "fail on first attempt" if retry_count.to_i < 1

    KafkaBatchSpec::ItestWorkers.write_marker!(job_id)
  end
end

class RubyAlwaysFailWorker
  include KafkaBatch::Worker

  job_type "integration.ruby_always_fail"
  max_retries 1
  complete_after_retries 1

  def perform(_payload)
    raise "always fails"
  end
end
