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

# Shared-name uniq worker. Both the Ruby client and the Go client enqueue
# job_type "integration.uniq_shared" resolving to worker_class "RubyUniqWorker",
# so the uniqueness fingerprint (worker_class name + canonical payload) is
# computed over the SAME material on both runtimes. This is what lets a job
# enqueued from one runtime dedupe against the same job enqueued from the other
# via the shared Redis lock. See integration/matrix/matrix_uniq_test.go.
class RubyUniqWorker
  include KafkaBatch::Worker

  job_type "integration.uniq_shared"
  uniq true

  def perform(_payload)
    KafkaBatchSpec::ItestWorkers.write_marker!(job_id)
  end
end
