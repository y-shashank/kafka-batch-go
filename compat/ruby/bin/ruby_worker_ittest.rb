#!/usr/bin/env ruby
# frozen_string_literal: true

# Minimal Ruby execution tier for cross-runtime integration tests.
# Consumes ruby job topics with the real JobConsumer and emits events to Kafka
# (Go control plane picks up events/callbacks).

require "yaml"
require "logger"
require "oj"
require "optparse"
require "securerandom"
require "set"

gem_root = ENV.fetch("KBATCH_RUBY_GEM_ROOT") do
  File.expand_path("../../kafka-batch", __dir__)
end
$LOAD_PATH.unshift File.join(gem_root, "lib") unless $LOAD_PATH.include?(File.join(gem_root, "lib"))

require "kafka_batch"
require_relative "../lib/kafka_batch_spec/itest_workers"

KMsg = Struct.new(:raw_payload, :topic, :partition, :offset, keyword_init: true)

def ruby_topics(cfg, manifest_path)
  topics = Set.new
  data = YAML.safe_load(File.read(manifest_path), permitted_classes: [], aliases: true) || {}
  handlers = data["handlers"] || {}
  handlers.each_value do |entry|
    entry = entry.transform_keys(&:to_s)
    next unless entry["runtime"] == "ruby"
    next if entry["fairness_type"] && !entry["fairness_type"].to_s.empty?

    t = entry["topic"].to_s
    topics << t unless t.empty?
  end
  %w[fairness_time_ready_ruby fairness_throughput_ready_ruby].each do |key|
    t = cfg[key]
    topics << t if t && !t.to_s.empty?
  end
  topics.to_a
end

options = {}
OptionParser.new do |opts|
  opts.on("--config PATH", "daemon YAML") { |v| options[:config] = v }
  opts.on("--manifest PATH", "handler manifest YAML") { |v| options[:manifest] = v }
end.parse!

abort "--config required" unless options[:config]
abort "--manifest required" unless options[:manifest]

cfg = YAML.safe_load(File.read(options[:config]), permitted_classes: [], aliases: true) || {}
brokers = (cfg["brokers"] || ["localhost:9092"]).join(",")
ready_file = ENV["KBATCH_RUBY_WORKER_READY_FILE"]
suffix = SecureRandom.hex(4)

KafkaBatch.reset!
KafkaBatch.configure do |c|
  c.brokers = brokers.split(",")
  c.logger = Logger.new($stderr, level: Logger::WARN)
  c.redis_url = ENV.fetch("REDIS_URL", cfg["redis_url"] || "redis://127.0.0.1:6379/0")
  c.events_topic = cfg["events_topic"]
  c.callbacks_topic = cfg["callbacks_topic"]
  c.dead_letter_topic = cfg["dead_letter_topic"]
  c.retry_topic = cfg["retry_topic"]
  c.max_retries = cfg.fetch("max_retries", 2)
	c.handler_manifest_path = options[:manifest]
	c.skip_cancelled_jobs = cfg.fetch("skip_cancelled_jobs", true)
  c.fair_time_ready_ruby_topic = cfg["fairness_time_ready_ruby"]
  c.fair_throughput_ready_ruby_topic = cfg["fairness_throughput_ready_ruby"]
  c.track_running_jobs = false
end

KafkaBatch::HandlerManifest.load!(options[:manifest])

topics = ruby_topics(cfg, options[:manifest])
abort "no ruby consume topics in manifest/config" if topics.empty?

$stderr.puts "[ruby-worker-ittest] consuming topics: #{topics.join(', ')}"

rd_cfg = Rdkafka::Config.new(
  :"bootstrap.servers" => brokers,
  :"group.id" => "kb-ruby-itest-#{suffix}",
  :"auto.offset.reset" => "earliest",
  :"enable.auto.commit" => false
)
consumer = rd_cfg.consumer
topics.each { |t| consumer.subscribe(t) }

sleep 2 # allow consumer group join before producers enqueue

File.write(ready_file, "ok") if ready_file && !ready_file.empty?

running = true
trap("TERM") { running = false }
trap("INT")  { running = false }

while running
  raw = consumer.poll(1_000)
  unless raw
    next
  end

  $stderr.puts "[ruby-worker-ittest] polled topic=#{raw.topic} offset=#{raw.offset} bytes=#{raw.payload&.bytesize}"

  committed = false
  job_consumer = KafkaBatch::Consumers::JobConsumer.allocate
  job_consumer.define_singleton_method(:mark_as_consumed!) { committed = true }
  job_consumer.define_singleton_method(:pause) { |_duration = nil| }
  job_consumer.define_singleton_method(:messages) do
    [KMsg.new(raw_payload: raw.payload, topic: raw.topic, partition: raw.partition, offset: raw.offset)]
  end

  begin
    job_consumer.process_messages
    $stderr.puts "[ruby-worker-ittest] processed committed=#{committed}"
  rescue Exception => e
    warn "[ruby-worker-ittest] consume error: #{e.class}: #{e.message}"
    warn e.backtrace.first(8).join("\n")
  end

  next unless committed

  begin
    consumer.commit
  rescue Rdkafka::RdkafkaError
    # no_offset — ignore
  end
end

consumer.close
