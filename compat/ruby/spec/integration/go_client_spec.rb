# frozen_string_literal: true

require "yaml"
require "fileutils"

require_relative "../support/go_daemon_helper"

RSpec.describe "Go produce client (integration)", :integration do
  include KafkaBatchSpec::GoWorkerLifecycle

  def configured_brokers
    ENV["KAFKA_BATCH_TEST_BROKERS"].to_s
  end

  def opted_in?
    ENV["KAFKA_BATCH_INTEGRATION"] == "1" || !configured_brokers.empty?
  end

  def brokers
    @brokers ||= configured_brokers.empty? ? "localhost:9092" : configured_brokers
  end

  def suffix
    @suffix ||= SecureRandom.hex(6)
  end

  def client_binary
    ENV.fetch("KBATCH_CLIENT_ITEST_BIN") do
      File.expand_path("../../bin/kbatch-client-ittest", __dir__)
    end
  end

  def go_available?
    File.executable?(client_binary) || system("which go >/dev/null 2>&1")
  end

  before(:each) do
    skip "set KAFKA_BATCH_INTEGRATION=1 to run" unless opted_in?
    require "rdkafka"
    skip "no Kafka broker reachable at #{brokers}" unless broker_reachable?
    skip "Go toolchain unavailable" unless go_available?

    @tmpdir = Dir.mktmpdir("kbatch-client-#{suffix}")
    @marker_path = File.join(@tmpdir, "marker")
    @out_path = File.join(@tmpdir, "out")
    @worker_topic = "kb.client.worker.#{suffix}"
    @events_topic = "kb.client.events.#{suffix}"
    @callbacks_topic = "kb.client.callbacks.#{suffix}"
    @dlt_topic = "kb.client.dlt.#{suffix}"
    @scheduled_topic = "kb.client.scheduled.#{suffix}"
    @retry_base = "kb.client.retry.#{suffix}"

    write_manifest!
    write_daemon_config!

    [@worker_topic, @events_topic, @callbacks_topic, @dlt_topic, @scheduled_topic,
     "#{@retry_base}.short", "#{@retry_base}.medium", "#{@retry_base}.large"].each do |t|
      create_topic!(t)
    end

    configure_kafka_batch!
    start_go_stack!
  end

  after(:each) do
    stop_go_stack! if @daemon_pid
    FileUtils.rm_rf(@tmpdir) if @tmpdir
    KafkaBatch::Producer.reset! if opted_in?
  end

  def write_manifest!
    @manifest_path = File.join(@tmpdir, "handlers.yml")
    File.write(@manifest_path, {
      "handlers" => {
        "integration.go_daemon" => {
          "runtime" => "go",
          "topic" => @worker_topic,
          "apply_topic_prefix" => false,
          "max_retries" => 1
        }
      }
    }.to_yaml)
  end

  def write_daemon_config!
    @ready_path = File.join(@tmpdir, "ready")
    @config_path = File.join(@tmpdir, "daemon.yml")
    File.write(@config_path, {
      "brokers" => brokers.split(","),
      "consumer_group" => "kb-client-#{suffix}",
      "jobs_topics" => [@worker_topic],
      "events_topic" => @events_topic,
      "callbacks_topic" => @callbacks_topic,
      "dead_letter_topic" => @dlt_topic,
      "scheduled_topic" => @scheduled_topic,
      "retry_topic" => @retry_base,
      "redis_url" => KafkaBatchSpec::RedisHelper::TEST_URL,
      "handler_manifest" => @manifest_path,
      "schedule_poller_enabled" => true,
      "max_retries" => 1,
      "retry_tiers" => { "short" => 0, "medium" => 0, "large" => 0 }
    }.to_yaml)
  end

  def configure_kafka_batch!
    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.brokers = brokers.split(",")
      c.logger = Logger.new(File::NULL)
      c.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      c.handler_manifest_path = @manifest_path
      c.callbacks_topic = @callbacks_topic
    end
    KafkaBatch::HandlerManifest.load!(@manifest_path)
    KafkaBatchSpec::RedisHelper.flush!
    KafkaBatch::Producer.reset!
  end

  def broker_reachable?
    cfg = Rdkafka::Config.new(:"bootstrap.servers" => brokers)
    admin = cfg.admin
    admin.metadata(nil, 3_000)
    true
  rescue StandardError
    false
  ensure
    admin&.close
  end

  def create_topic!(name, partitions: 1)
    cfg   = Rdkafka::Config.new(:"bootstrap.servers" => brokers)
    admin = cfg.admin
    admin.create_topic(name, partitions, 1).wait(max_wait_timeout: 15)
  rescue Rdkafka::RdkafkaError => e
    raise unless e.message.to_s =~ /exist/i
  ensure
    admin&.close
  end

  def run_client!(mode:)
    env = ENV.to_h.merge(
      "KBATCH_CLIENT_BROKERS" => brokers,
      "KBATCH_CLIENT_REDIS" => KafkaBatchSpec::RedisHelper::TEST_URL,
      "KBATCH_CLIENT_MANIFEST" => @manifest_path,
      "KBATCH_CLIENT_MARKER" => @marker_path,
      "KBATCH_CLIENT_OUT" => @out_path
    )
    cmd = if File.executable?(client_binary)
            [client_binary, "-mode", mode]
          else
            ["go", "run", "./cmd/kbatch-client-ittest", "-mode", mode]
          end
    unless system(env, *cmd, chdir: File.expand_path("../../../..", __dir__))
      raise "client itest failed (mode=#{mode})"
    end
  end

  def wait_for_marker!(timeout: 45)
    deadline = Time.now + timeout
    loop do
      return File.read(@marker_path).strip if File.exist?(@marker_path) && !File.read(@marker_path).strip.empty?
      raise "timeout waiting for marker" if Time.now >= deadline

      sleep 0.25
    end
  end

  def wait_for_batch!(batch_id, timeout: 45)
    deadline = Time.now + timeout
    loop do
      row = KafkaBatch.store.find_batch(batch_id)
      return row if row && %w[success complete].include?(row[:status].to_s)

      raise "timeout waiting for batch #{batch_id}" if Time.now >= deadline

      sleep 0.25
    end
  end

  it "enqueues a standalone job consumed by kbatch worker" do
    run_client!(mode: "enqueue")
    job_id = wait_for_marker!
    expect(job_id).not_to be_empty
    expect(File.read(@out_path).strip).to eq(job_id)
  end

  it "push_many completes a batch via the go worker" do
    run_client!(mode: "batch-many")
    batch_id = wait_for_marker!
    row = wait_for_batch!(batch_id)
    expect(row[:total_jobs]).to eq(2)
    expect(row[:completed_count]).to eq(2)
  end

  it "push_many_at dispatches scheduled jobs when due" do
    run_client!(mode: "scheduled-many")
    batch_id = wait_for_marker!
    row = wait_for_batch!(batch_id, timeout: 60)
    expect(row[:total_jobs]).to eq(1)
    expect(row[:completed_count]).to eq(1)
  end
end
