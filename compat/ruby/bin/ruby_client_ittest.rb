#!/usr/bin/env ruby
# frozen_string_literal: true

# Ruby produce client for cross-runtime matrix tests.
# Mirrors kbatch-client-ittest modes; writes JSON result to KBATCH_CLIENT_OUT.

require "json"
require "yaml"
require "optparse"
require "securerandom"

gem_root = ENV.fetch("KBATCH_RUBY_GEM_ROOT") do
  File.expand_path("../../kafka-batch", __dir__)
end
$LOAD_PATH.unshift File.join(gem_root, "lib") unless $LOAD_PATH.include?(File.join(gem_root, "lib"))

require "kafka_batch"
require_relative "../lib/kafka_batch_spec/itest_workers"

options = { mode: "enqueue", job_type: "integration.go_daemon" }
OptionParser.new do |opts|
  opts.on("--config PATH", "daemon YAML") { |v| options[:config] = v }
  opts.on("--manifest PATH", "handler manifest YAML") { |v| options[:manifest] = v }
  opts.on("--mode MODE", "enqueue | batch-go | batch-ruby | batch-mixed | scheduled-go") { |v| options[:mode] = v }
  opts.on("--job-type TYPE") { |v| options[:job_type] = v }
end.parse!

abort "--config required" unless options[:config]
abort "--manifest required" unless options[:manifest]

cfg = YAML.safe_load(File.read(options[:config]), permitted_classes: [], aliases: true) || {}
brokers = (cfg["brokers"] || ["localhost:9092"]).join(",")
out_path = ENV["KBATCH_CLIENT_OUT"]
marker = ENV["KBATCH_CLIENT_MARKER"]

KafkaBatch.reset!
KafkaBatch.configure do |c|
  c.brokers = brokers.split(",")
  c.redis_url = ENV.fetch("REDIS_URL", cfg["redis_url"] || "redis://127.0.0.1:6379/0")
  c.handler_manifest_path = options[:manifest]
  c.events_topic = cfg["events_topic"]
  c.callbacks_topic = cfg["callbacks_topic"]
  c.dead_letter_topic = cfg["dead_letter_topic"]
  c.scheduled_topic = cfg["scheduled_topic"] if cfg["scheduled_topic"]
  c.fair_time_ingest_topic = cfg["fairness_time_ingest"]
  c.fair_throughput_ingest_topic = cfg["fairness_throughput_ingest"]
  c.uniq_enabled = cfg.fetch("uniq_enabled", false)
end
KafkaBatch::HandlerManifest.load!(options[:manifest])
KafkaBatch::Producer.reset!

result = {}

case options[:mode]
when "enqueue"
  job_id = KafkaBatch::Batch.enqueue_job(options[:job_type], { "n" => 1 })
  result = { "job_ids" => { "primary" => job_id } }
  File.write(marker, job_id) if marker && !marker.empty?
when "batch-go"
  go_id = nil
  batch = KafkaBatch::Batch.create(description: "ruby-client-go") do |b|
    go_id = b.push_job("integration.go_daemon", { "ping" => 1 })
  end
  result = { "batch_id" => batch.id, "job_ids" => { "go" => go_id } }
  File.write(marker, batch.id) if marker && !marker.empty?
when "batch-retry-go"
  go_id = nil
  batch = KafkaBatch::Batch.create(description: "ruby-client-retry") do |b|
    go_id = b.push_job("integration.go_retry_once", { "ping" => 1 })
  end
  result = { "batch_id" => batch.id, "job_ids" => { "go" => go_id } }
  File.write(marker, batch.id) if marker && !marker.empty?
when "batch-fail"
  batch = KafkaBatch::Batch.create(description: "ruby-client-dlt") do |b|
    b.push_job("integration.go_always_fail", { "x" => 1 })
  end
  result = { "batch_id" => batch.id, "job_ids" => {} }
  File.write(marker, batch.id) if marker && !marker.empty?
when "batch-ruby"
  ruby_id = nil
  batch = KafkaBatch::Batch.create(description: "ruby-client-ruby") do |b|
    ruby_id = b.push_job("integration.ruby_plain", { "order_id" => 1 })
  end
  result = { "batch_id" => batch.id, "job_ids" => { "ruby" => ruby_id } }
  File.write(marker, batch.id) if marker && !marker.empty?
when "batch-mixed"
  ids = {}
  batch = KafkaBatch::Batch.create(description: "ruby-client-mixed") do |b|
    ids["go"] = b.push_job("integration.go_multi", { "n" => 1 })
    ids["ruby"] = b.push_job("integration.ruby_plain", { "order_id" => 2 })
  end
  result = { "batch_id" => batch.id, "job_ids" => ids }
  File.write(marker, batch.id) if marker && !marker.empty?
when "scheduled-go"
  go_id = nil
  batch = KafkaBatch::Batch.create(description: "ruby-client-sched") do |b|
    definition = KafkaBatch::Batch.resolve_definition!("integration.go_scheduled")
    go_id = SecureRandom.uuid
    batch_seq = b.send(:reserve!, 1)
    message = KafkaBatch::Batch.build_message_for(
      definition: definition, payload: { "n" => 1 },
      job_id: go_id, batch_id: b.id, attempt: 0, batch_seq: batch_seq
    )
    KafkaBatch::Batch.schedule_message(message, run_at: Time.now + 2, batch_id: b.id)
  end
  result = { "batch_id" => batch.id, "job_ids" => { "go" => go_id } }
  File.write(marker, batch.id) if marker && !marker.empty?
else
  abort "unknown mode #{options[:mode]}"
end

payload = JSON.generate(result)
File.write(out_path, payload) if out_path && !out_path.empty?
puts payload
