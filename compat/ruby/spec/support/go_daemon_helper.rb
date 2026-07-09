# frozen_string_literal: true

module KafkaBatchSpec
  # Kafka poll helpers and Go worker lifecycle for integration specs.
  module GoDaemonHelper
    module_function

    def poll_topic(brokers:, topic:, group_suffix:, timeout: 30, match: nil)
      require "rdkafka"
      cfg = Rdkafka::Config.new(
        :"bootstrap.servers"  => brokers,
        :"group.id"           => "kb-daemon-poll-#{group_suffix}-#{SecureRandom.hex(4)}",
        :"auto.offset.reset"  => "earliest",
        :"enable.auto.commit" => false
      )
      consumer = cfg.consumer
      consumer.subscribe(topic)

      deadline = Time.now + timeout
      while Time.now < deadline
        raw = consumer.poll(1_000)
        next unless raw

        decoded = Oj.load(raw.payload)
        return decoded if match.nil? || match.call(decoded)
      end
      nil
    ensure
      consumer&.close
    end
  end

  # Mixin for specs that spawn kbatch go-worker alongside the control daemon.
  module GoWorkerLifecycle
    def worker_binary
      ENV.fetch("KBATCH_WORKER_ITEST_BIN") do
        File.expand_path("../../../../bin/kbatch-worker-ittest", __dir__)
      end
    end

    def go_repo_root
      File.expand_path("../../..", __dir__)
    end

    def start_go_stack!
      start_daemon!
      start_go_worker!
    end

    def stop_go_stack!
      stop_go_worker!
      stop_daemon!
    end

    def daemon_binary
      ENV.fetch("KBATCH_DAEMON_ITEST_BIN") do
        File.expand_path("../../../../bin/kbatch-daemon-ittest", __dir__)
      end
    end

    def start_daemon!
      @ready_path = File.join(@tmpdir, "daemon_ready") unless defined?(@ready_path) && @ready_path
      env = ENV.to_h.merge(
        "KBATCH_DAEMON_ITEST_MARKER" => @marker_path,
        "KBATCH_DAEMON_READY_FILE" => @ready_path,
        "REDIS_URL" => KafkaBatchSpec::RedisHelper::TEST_URL,
        "KAFKA_PREFIX" => ""
      )
      cmd = if File.executable?(daemon_binary)
              [daemon_binary, "--config", @config_path, "--manifest", @manifest_path]
            else
              ["go", "run", "./cmd/kbatch-daemon-ittest", "--config", @config_path, "--manifest", @manifest_path]
            end
      @daemon_pid = Process.spawn(env, *cmd, chdir: go_repo_root,
                                  out: File::NULL, err: File.join(@tmpdir, "daemon.err"))
      wait_for_daemon!
    end

    def wait_for_daemon!(timeout: 30)
      deadline = Time.now + timeout
      while Time.now < deadline
        return if File.exist?(@ready_path)
        Process.kill(0, @daemon_pid)
        sleep 0.2
      end
      err = File.exist?(File.join(@tmpdir, "daemon.err")) ? File.read(File.join(@tmpdir, "daemon.err")) : ""
      raise "daemon did not become ready within #{timeout}s\n#{err}"
    rescue Errno::ESRCH
      raise "daemon process died during startup"
    end

    def stop_daemon!
      return unless @daemon_pid

      Process.kill("TERM", @daemon_pid)
      Timeout.timeout(5) { Process.wait(@daemon_pid) }
    rescue Errno::ESRCH, Timeout::Error
      Process.kill("KILL", @daemon_pid) rescue nil
    ensure
      @daemon_pid = nil
    end

    def start_go_worker!
      @worker_ready_path = File.join(@tmpdir, "worker_ready")
      env = ENV.to_h.merge(
        "KBATCH_DAEMON_ITEST_MARKER" => @marker_path,
        "KBATCH_DAEMON_ITEST_MARKER_P0" => (@p0_marker || @marker_path),
        "KBATCH_DAEMON_ITEST_MARKER_TP" => @marker_path,
        "KBATCH_WORKER_READY_FILE" => @worker_ready_path,
        "REDIS_URL" => KafkaBatchSpec::RedisHelper::TEST_URL,
        "KAFKA_PREFIX" => ""
      )
      cmd = if File.executable?(worker_binary)
              [worker_binary, "--config", @config_path, "--manifest", @manifest_path]
            else
              ["go", "run", "./cmd/kbatch-worker-ittest", "--config", @config_path, "--manifest", @manifest_path]
            end
      @worker_pid = Process.spawn(env, *cmd, chdir: go_repo_root,
                                  out: File::NULL, err: File.join(@tmpdir, "worker.err"))
      wait_for_go_worker!
    end

    def wait_for_go_worker!(timeout: 30)
      deadline = Time.now + timeout
      while Time.now < deadline
        return if File.exist?(@worker_ready_path)
        Process.kill(0, @worker_pid)
        sleep 0.2
      end
      err = File.exist?(File.join(@tmpdir, "worker.err")) ? File.read(File.join(@tmpdir, "worker.err")) : ""
      raise "go worker did not become ready within #{timeout}s\n#{err}"
    rescue Errno::ESRCH
      raise "go worker process died during startup"
    end

    def stop_go_worker!
      return unless @worker_pid

      Process.kill("TERM", @worker_pid)
      Timeout.timeout(5) { Process.wait(@worker_pid) }
    rescue Errno::ESRCH, Timeout::Error
      Process.kill("KILL", @worker_pid) rescue nil
    ensure
      @worker_pid = nil
    end
  end
end
