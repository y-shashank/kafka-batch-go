#!/usr/bin/env ruby
# frozen_string_literal: true

# Drains pending ruby job messages using the real JobConsumer (one poll loop).
# Invoked by Go matrix tests after jobs are enqueued.

require "yaml"
require "logger"
require "oj"
require "optparse"
require "set"
require "securerandom"

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

options = { timeout: 45, idle: 3 }
OptionParser.new do |opts|
  opts.on("--config PATH", "daemon YAML") { |v| options[:config] = v }
  opts.on("--manifest PATH", "handler manifest YAML") { |v| options[:manifest] = v }
  opts.on("--timeout SEC", Integer, "max seconds") { |v| options[:timeout] = v }
  opts.on("--idle SEC", Integer, "stop after idle seconds") { |v| options[:idle] = v }
end.parse!

abort "--config required" unless options[:config]
abort "--manifest required" unless options[:manifest]

cfg = YAML.safe_load(File.read(options[:config]), permitted_classes: [], aliases: true) || {}
brokers = (cfg["brokers"] || ["localhost:9092"]).join(",")

KafkaBatch.reset!
KafkaBatch.configure do |c|
  c.brokers = brokers.split(",")
  c.logger = Logger.new($stderr, level: Logger::WARN)
  c.redis_url = ENV.fetch("REDIS_URL", cfg["redis_url"] || "redis://127.0.0.1:6379/0")
  c.events_topic = cfg["events_topic"]
  c.callbacks_topic = cfg["callbacks_topic"]
  c.dead_letter_topic = cfg["dead_letter_topic"]
  c.retry_topic = cfg["retry_topic"]
  tiers = cfg["retry_tiers"] || {}
  unless tiers.empty?
    c.retry_tiers = tiers.transform_keys(&:to_sym).transform_values(&:to_i)
  end
  c.max_retries = cfg.fetch("max_retries", 2)
  c.complete_after_retries = cfg.fetch("complete_after_retries", 1)
  c.handler_manifest_path = options[:manifest]
  c.skip_cancelled_jobs = cfg.fetch("skip_cancelled_jobs", true)
  c.fair_time_ready_ruby_topic = cfg["fairness_time_ready_ruby"]
  c.fair_throughput_ready_ruby_topic = cfg["fairness_throughput_ready_ruby"]
  c.track_running_jobs = false
end

KafkaBatch::HandlerManifest.load!(options[:manifest])

topics = if ENV["KBATCH_RUBY_DRAIN_TOPICS"] && !ENV["KBATCH_RUBY_DRAIN_TOPICS"].empty?
           ENV["KBATCH_RUBY_DRAIN_TOPICS"].split(",").map(&:strip).reject(&:empty?)
         else
           ruby_topics(cfg, options[:manifest])
         end
abort "no ruby topics" if topics.empty?

suffix = SecureRandom.hex(4)
rd = Rdkafka::Config.new(
  :"bootstrap.servers" => brokers,
  :"group.id" => "kb-ruby-drain-#{suffix}",
  :"auto.offset.reset" => "earliest",
  :"enable.auto.commit" => false
).consumer
topics.each { |t| rd.subscribe(t) }
$stderr.puts "[ruby-drain] topics: #{topics.join(', ')}"

# Allow group join + partition assignment without consuming messages (poll would drop records).
sleep 5

processed = 0
deadline = Time.now + options[:timeout]
last_msg = Time.now

while Time.now < deadline
  raw = rd.poll(500)
  unless raw
    break if processed.positive? && (Time.now - last_msg) >= options[:idle]

    next
  end

  last_msg = Time.now
  consumer = KafkaBatch::Consumers::JobConsumer.allocate
  committed = false
  consumer.define_singleton_method(:mark_as_consumed!) { |*_args| committed = true }
  consumer.define_singleton_method(:pause) { |*_args| }
  consumer.define_singleton_method(:messages) do
    [KMsg.new(raw_payload: raw.payload, topic: raw.topic, partition: raw.partition, offset: raw.offset)]
  end

  begin
    consumer.process_messages
  rescue Exception => e
    warn "[ruby-drain] consume error: #{e.class}: #{e.message}"
  end

  if committed
    rd.commit
    processed += 1
    $stderr.puts "[ruby-drain] processed topic=#{raw.topic} offset=#{raw.offset} total=#{processed}"
  else
    $stderr.puts "[ruby-drain] skipped commit topic=#{raw.topic} offset=#{raw.offset}"
  end
end

rd.close
$stderr.puts "[ruby-drain] done processed=#{processed}"
