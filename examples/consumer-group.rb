$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require "eb-kafka"

logger = Logger.new(STDOUT)
brokers = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")

# Make sure to create this topic in your EbKafka cluster or configure the
# cluster to auto-create topics.
topic = "text"

kafka = EbKafka.new(
  seed_brokers: brokers,
  client_id: "test",
  socket_timeout: 20,
  logger: logger,
)

consumer = kafka.consumer(group_id: "test")
consumer.subscribe(topic)

trap("TERM") { consumer.stop }
trap("INT") { consumer.stop }

consumer.each_message do |message|
  puts message.value
end
