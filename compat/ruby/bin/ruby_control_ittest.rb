#!/usr/bin/env ruby
# frozen_string_literal: true

# Ruby Karafka control plane for cross-runtime matrix tests.
# One rdkafka consumer thread per control topic (avoids multi-subscribe assignment gaps).

require "yaml"
require "logger"
require "optparse"
require "set"
require "securerandom"

gem_root = ENV.fetch("KBATCH_RUBY_GEM_ROOT") do
  File.expand_path("../../kafka-batch", __dir__)
end
compat = File.expand_path("..", __dir__)
$LOAD_PATH.unshift File.join(gem_root, "lib") unless $LOAD_PATH.include?(File.join(gem_root, "lib"))

require "kafka_batch"
require File.join(compat, "lib/kafka_batch_spec/itest_workers")

KMsg = Struct.new(:raw_payload, :topic, :partition, :offset, keyword_init: true)

def control_topics(cfg)
  topics = Set.new
  %w[events_topic callbacks_topic].each do |key|
    t = cfg[key]
    topics << t if t && !t.to_s.empty?
  end
  base = cfg["retry_topic"]
  if base && !base.to_s.empty?
    %w[short medium large].each { |tier| topics << "#{base}.#{tier}" }
  end
  %w[fairness_time_ingest fairness_throughput_ingest].each do |key|
    t = cfg[key]
    topics << t if t && !t.to_s.empty?
  end
  topics.to_a
end

def route_for(topic, cfg)
  case topic
  when cfg["events_topic"]
    [KafkaBatch::Consumers::EventConsumer, :consume]
  when cfg["callbacks_topic"]
    [KafkaBatch::Consumers::CallbackConsumer, :consume]
  when "#{cfg['retry_topic']}.short", "#{cfg['retry_topic']}.medium", "#{cfg['retry_topic']}.large"
    [KafkaBatch::Consumers::RetryConsumer, :consume]
  when cfg["fairness_time_ingest"], cfg["fairness_throughput_ingest"]
    [KafkaBatch::Fairness::Dispatcher, :consume]
  end
end

def dispatch_message(raw, cfg)
  route = route_for(raw.topic, cfg)
  return unless route

  klass, method = route
  inst = klass.allocate
  committed_box = [false]
  topic_view = Struct.new(:name, :consumer_group).new(
    raw.topic,
    Struct.new(:id).new("kb-ruby-control")
  )
  inst.define_singleton_method(:mark_as_consumed!) { |*_args| committed_box[0] = true }
  inst.define_singleton_method(:pause) { |*_args| }
  inst.define_singleton_method(:topic) { topic_view }
  inst.define_singleton_method(:partition) { raw.partition }
  inst.define_singleton_method(:messages) do
    [KMsg.new(raw_payload: raw.payload, topic: raw.topic, partition: raw.partition, offset: raw.offset)]
  end

  begin
    inst.public_send(method)
  rescue Exception => e
    warn "[ruby-control] #{klass.name} error: #{e.class}: #{e.message}"
    warn e.backtrace.first(5).join("\n")
  end

  if committed_box[0]
    $stderr.puts "[ruby-control] committed topic=#{raw.topic} offset=#{raw.offset}"
  end
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
ready_file = ENV["KBATCH_RUBY_CONTROL_READY_FILE"]

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
  c.retry_tiers = tiers.transform_keys(&:to_sym).transform_values(&:to_i) unless tiers.empty?
  c.max_retries = cfg.fetch("max_retries", 2)
  c.complete_after_retries = cfg.fetch("complete_after_retries", 1)
  c.handler_manifest_path = options[:manifest]
  c.scheduled_topic = cfg["scheduled_topic"] if cfg["scheduled_topic"]
  c.schedule_poller_enabled = cfg.fetch("schedule_poller_enabled", false)
  c.schedule_poll_interval = cfg.fetch("schedule_poll_interval", 0.5)
  c.fair_time_ingest_topic = cfg["fairness_time_ingest"]
  c.fair_time_ready_go_topic = cfg["fairness_time_ready_go"]
  c.fair_time_ready_ruby_topic = cfg["fairness_time_ready_ruby"]
  c.fair_throughput_ingest_topic = cfg["fairness_throughput_ingest"]
  c.fair_throughput_ready_go_topic = cfg["fairness_throughput_ready_go"]
  c.fair_throughput_ready_ruby_topic = cfg["fairness_throughput_ready_ruby"]
  c.skip_cancelled_jobs = cfg.fetch("skip_cancelled_jobs", true)
  c.validate_topics_on_boot = false
end
KafkaBatch::HandlerManifest.load!(options[:manifest])
KafkaBatch::SchedulePoller.ensure_running! if KafkaBatch.config.schedule_poller_enabled

topics = control_topics(cfg)
abort "no control topics configured" if topics.empty?

$stderr.puts "[ruby-control] topics: #{topics.join(', ')}"

running = true
trap("TERM") { running = false }
trap("INT")  { running = false }

threads = topics.map do |topic|
  Thread.new do
    suffix = SecureRandom.hex(3)
    rd = Rdkafka::Config.new(
      :"bootstrap.servers" => brokers,
      :"group.id" => "kb-ruby-ctl-#{suffix}",
      :"auto.offset.reset" => "earliest",
      :"enable.auto.commit" => false
    ).consumer
    rd.subscribe(topic)
    while running
      begin
        raw = rd.poll(500)
      rescue Rdkafka::RdkafkaError => e
        warn "[ruby-control] poll #{topic}: #{e.message}"
        next
      end
      next unless raw

      dispatch_message(raw, cfg)
      rd.commit
    end
    rd.close
  end
end

sleep 3
File.write(ready_file, "ok") if ready_file && !ready_file.empty?

threads.each(&:join)
