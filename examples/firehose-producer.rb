$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require "eb-kafka"
require "dotenv"

Dotenv.load

KAFKA_CLIENT_CERT = ENV.fetch("eb-kafka_CLIENT_CERT")
KAFKA_CLIENT_CERT_KEY = ENV.fetch("eb-kafka_CLIENT_CERT_KEY")
KAFKA_SERVER_CERT = ENV.fetch("eb-kafka_SERVER_CERT")
KAFKA_URL = ENV.fetch("eb-kafka_URL")
KAFKA_BROKERS = KAFKA_URL
KAFKA_TOPIC = "test-messages"

NUM_THREADS = 20

threads = NUM_THREADS.times.map do
  Thread.new do
    logger = Logger.new($stderr)
    logger.level = Logger::INFO

    kafka = EbKafka.new(
      seed_brokers: KAFKA_BROKERS,
      logger: logger,
      ssl_client_cert: KAFKA_CLIENT_CERT,
      ssl_client_cert_key: KAFKA_CLIENT_CERT_KEY,
      ssl_ca_cert: KAFKA_SERVER_CERT,
    )

    producer = kafka.async_producer(
      delivery_interval: 1,
      max_queue_size: 5_000,
      max_buffer_size: 10_000,
    )

    begin
      loop do
        producer.produce(rand.to_s, key: rand.to_s, topic: KAFKA_TOPIC)
      end
    rescue EbKafka::BufferOverflow
      logger.error "Buffer overflow, backing off for 1s"
      sleep 1
      retry
    ensure
      producer.shutdown
    end
  end
end

threads.each {|t| t.abort_on_exception = true }

threads.map(&:join)
