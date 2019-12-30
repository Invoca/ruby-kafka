require "set"
require "eb-kafka/partitioner"
require "eb-kafka/message_buffer"
require "eb-kafka/produce_operation"
require "eb-kafka/pending_message_queue"
require "eb-kafka/pending_message"
require "eb-kafka/compressor"

module EbKafka

  # Allows sending messages to a EbKafka cluster.
  #
  # Typically you won't instantiate this class yourself, but rather have {EbKafka::Client}
  # do it for you, e.g.
  #
  #     # Will instantiate EbKafka::Client
  #     kafka = EbKafka.new(["kafka1:9092", "kafka2:9092"])
  #
  #     # Will instantiate EbKafka::Producer
  #     producer = kafka.producer
  #
  # This is done in order to share a logger as well as a pool of broker connections across
  # different producers. This also means that you don't need to pass the `cluster` and
  # `logger` options to `#producer`. See {#initialize} for the list of other options
  # you can pass in.
  #
  # ## Buffering
  #
  # The producer buffers pending messages until {#deliver_messages} is called. Note that there is
  # a maximum buffer size (default is 1,000 messages) and writing messages after the
  # buffer has reached this size will result in a BufferOverflow exception. Make sure
  # to periodically call {#deliver_messages} or set `max_buffer_size` to an appropriate value.
  #
  # Buffering messages and sending them in batches greatly improves performance, so
  # try to avoid sending messages after every write. The tradeoff between throughput and
  # message delays depends on your use case.
  #
  # ## Error Handling and Retries
  #
  # The design of the error handling is based on having a {MessageBuffer} hold messages
  # for all topics/partitions. Whenever we want to send messages to the cluster, we
  # group the buffered messages by the broker they need to be sent to and fire off a
  # request to each broker. A request can be a partial success, so we go through the
  # response and inspect the error code for each partition that we wrote to. If the
  # write to a given partition was successful, we clear the corresponding messages
  # from the buffer -- otherwise, we log the error and keep the messages in the buffer.
  #
  # After this, we check if the buffer is empty. If it is, we're all done. If it's
  # not, we do another round of requests, this time with just the remaining messages.
  # We do this for as long as `max_retries` permits.
  #
  # ## Compression
  #
  # Depending on what kind of data you produce, enabling compression may yield improved
  # bandwidth and space usage. Compression in EbKafka is done on entire messages sets
  # rather than on individual messages. This improves the compression rate and generally
  # means that compressions works better the larger your buffers get, since the message
  # sets will be larger by the time they're compressed.
  #
  # Since many workloads have variations in throughput and distribution across partitions,
  # it's possible to configure a threshold for when to enable compression by setting
  # `compression_threshold`. Only if the defined number of messages are buffered for a
  # partition will the messages be compressed.
  #
  # Compression is enabled by passing the `compression_codec` parameter with the
  # name of one of the algorithms allowed by EbKafka:
  #
  # * `:snappy` for [Snappy](http://google.github.io/snappy/) compression.
  # * `:gzip` for [gzip](https://en.wikipedia.org/wiki/Gzip) compression.
  #
  # By default, all message sets will be compressed if you specify a compression
  # codec. To increase the compression threshold, set `compression_threshold` to
  # an integer value higher than one.
  #
  # ## Instrumentation
  #
  # Whenever {#produce} is called, the notification `produce_message.producer.kafka`
  # will be emitted with the following payload:
  #
  # * `value` – the message value.
  # * `key` – the message key.
  # * `topic` – the topic that was produced to.
  # * `buffer_size` – the buffer size after adding the message.
  # * `max_buffer_size` – the maximum allowed buffer size for the producer.
  #
  # After {#deliver_messages} completes, the notification
  # `deliver_messages.producer.kafka` will be emitted with the following payload:
  #
  # * `message_count` – the total number of messages that the producer tried to
  #   deliver. Note that not all messages may get delivered.
  # * `delivered_message_count` – the number of messages that were successfully
  #   delivered.
  # * `attempts` – the number of attempts made to deliver the messages.
  #
  # ## Example
  #
  # This is an example of an application which reads lines from stdin and writes them
  # to EbKafka:
  #
  #     require "eb-kafka"
  #
  #     logger = Logger.new($stderr)
  #     brokers = ENV.fetch("KAFKA_BROKERS").split(",")
  #
  #     # Make sure to create this topic in your EbKafka cluster or configure the
  #     # cluster to auto-create topics.
  #     topic = "random-messages"
  #
  #     kafka = EbKafka.new(brokers, client_id: "simple-producer", logger: logger)
  #     producer = kafka.producer
  #
  #     begin
  #       $stdin.each_with_index do |line, index|
  #         producer.produce(line, topic: topic)
  #
  #         # Send messages for every 10 lines.
  #         producer.deliver_messages if index % 10 == 0
  #       end
  #     ensure
  #       # Make sure to send any remaining messages.
  #       producer.deliver_messages
  #
  #       producer.shutdown
  #     end
  #
  class Producer

    def initialize(cluster:, logger:, instrumenter:, compressor:, ack_timeout:, required_acks:, max_retries:, retry_backoff:, max_buffer_size:, max_buffer_bytesize:)
      @cluster = cluster
      @logger = logger
      @instrumenter = instrumenter
      @required_acks = required_acks == :all ? -1 : required_acks
      @ack_timeout = ack_timeout
      @max_retries = max_retries
      @retry_backoff = retry_backoff
      @max_buffer_size = max_buffer_size
      @max_buffer_bytesize = max_buffer_bytesize
      @compressor = compressor

      # The set of topics that are produced to.
      @target_topics = Set.new

      # A buffer organized by topic/partition.
      @buffer = MessageBuffer.new

      # Messages added by `#produce` but not yet assigned a partition.
      @pending_message_queue = PendingMessageQueue.new
    end

    # Produces a message to the specified topic. Note that messages are buffered in
    # the producer until {#deliver_messages} is called.
    #
    # ## Partitioning
    #
    # There are several options for specifying the partition that the message should
    # be written to.
    #
    # The simplest option is to not specify a message key, partition key, or
    # partition number, in which case the message will be assigned a partition at
    # random.
    #
    # You can also specify the `partition` parameter yourself. This requires you to
    # know which partitions are available, however. Oftentimes the best option is
    # to specify the `partition_key` parameter: messages with the same partition
    # key will always be assigned to the same partition, as long as the number of
    # partitions doesn't change. You can also omit the partition key and specify
    # a message key instead. The message key is part of the message payload, and
    # so can carry semantic value--whether you want to have the message key double
    # as a partition key is up to you.
    #
    # @param value [String] the message data.
    # @param key [String] the message key.
    # @param topic [String] the topic that the message should be written to.
    # @param partition [Integer] the partition that the message should be written to.
    # @param partition_key [String] the key that should be used to assign a partition.
    # @param create_time [Time] the timestamp that should be set on the message.
    #
    # @raise [BufferOverflow] if the maximum buffer size has been reached.
    # @return [nil]
    def produce(value, key: nil, topic:, partition: nil, partition_key: nil, create_time: Time.now)
      message = PendingMessage.new(
        value && value.to_s,
        key && key.to_s,
        topic.to_s,
        partition && Integer(partition),
        partition_key && partition_key.to_s,
        create_time,
      )

      if buffer_size >= @max_buffer_size
        buffer_overflow topic,
          "Cannot produce to #{topic}, max buffer size (#{@max_buffer_size} messages) reached"
      end

      if buffer_bytesize + message.bytesize >= @max_buffer_bytesize
        buffer_overflow topic,
          "Cannot produce to #{topic}, max buffer bytesize (#{@max_buffer_bytesize} bytes) reached"
      end

      @target_topics.add(topic)
      @pending_message_queue.write(message)

      @instrumenter.instrument("produce_message.producer", {
        value: value,
        key: key,
        topic: topic,
        create_time: create_time,
        message_size: message.bytesize,
        buffer_size: buffer_size,
        max_buffer_size: @max_buffer_size,
      })

      nil
    end

    # Sends all buffered messages to the EbKafka brokers.
    #
    # Depending on the value of `required_acks` used when initializing the producer,
    # this call may block until the specified number of replicas have acknowledged
    # the writes. The `ack_timeout` setting places an upper bound on the amount of
    # time the call will block before failing.
    #
    # @raise [DeliveryFailed] if not all messages could be successfully sent.
    # @return [nil]
    def deliver_messages
      # There's no need to do anything if the buffer is empty.
      return if buffer_size == 0

      @instrumenter.instrument("deliver_messages.producer") do |notification|
        message_count = buffer_size

        notification[:message_count] = message_count
        notification[:attempts] = 0

        begin
          deliver_messages_with_retries(notification)
        ensure
          notification[:delivered_message_count] = message_count - buffer_size
        end
      end
    end

    # Returns the number of messages currently held in the buffer.
    #
    # @return [Integer] buffer size.
    def buffer_size
      @pending_message_queue.size + @buffer.size
    end

    def buffer_bytesize
      @pending_message_queue.bytesize + @buffer.bytesize
    end

    # Deletes all buffered messages.
    #
    # @return [nil]
    def clear_buffer
      @buffer.clear
      @pending_message_queue.clear
    end

    # Closes all connections to the brokers.
    #
    # @return [nil]
    def shutdown
      @cluster.disconnect
    end

    private

    def deliver_messages_with_retries(notification)
      attempt = 0

      @cluster.add_target_topics(@target_topics)

      operation = ProduceOperation.new(
        cluster: @cluster,
        buffer: @buffer,
        required_acks: @required_acks,
        ack_timeout: @ack_timeout,
        compressor: @compressor,
        logger: @logger,
        instrumenter: @instrumenter,
      )

      loop do
        attempt += 1

        notification[:attempts] = attempt

        begin
          @cluster.refresh_metadata_if_necessary!
        rescue ConnectionError => e
          raise DeliveryFailed.new(e, buffer_messages)
        end

        assign_partitions!
        operation.execute

        if @required_acks.zero?
          # No response is returned by the brokers, so we can't know which messages
          # have been successfully written. Our only option is to assume that they all
          # have.
          @buffer.clear
        end

        if buffer_size.zero?
          break
        elsif attempt <= @max_retries
          @logger.warn "Failed to send all messages; attempting retry #{attempt} of #{@max_retries} after #{@retry_backoff}s"

          sleep @retry_backoff
        else
          @logger.error "Failed to send all messages; keeping remaining messages in buffer"
          break
        end
      end

      unless @pending_message_queue.empty?
        # Mark the cluster as stale in order to force a cluster metadata refresh.
        @cluster.mark_as_stale!
        raise DeliveryFailed.new("Failed to assign partitions to #{@pending_message_queue.size} messages", buffer_messages)
      end

      unless @buffer.empty?
        partitions = @buffer.map {|topic, partition, _| "#{topic}/#{partition}" }.join(", ")

        raise DeliveryFailed.new("Failed to send messages to #{partitions}", buffer_messages)
      end
    end

    def assign_partitions!
      failed_messages = []
      topics_with_failures = Set.new

      @pending_message_queue.each do |message|
        partition = message.partition

        begin
          # If a message for a topic fails to receive a partition all subsequent
          # messages for the topic should be retried to preserve ordering
          if topics_with_failures.include?(message.topic)
            failed_messages << message
            next
          end

          if partition.nil?
            partition_count = @cluster.partitions_for(message.topic).count
            partition = Partitioner.partition_for_key(partition_count, message)
          end

          @buffer.write(
            value: message.value,
            key: message.key,
            topic: message.topic,
            partition: partition,
            create_time: message.create_time,
          )
        rescue EbKafka::Error => e
          @instrumenter.instrument("topic_error.producer", {
            topic: message.topic,
            exception: [e.class.to_s, e.message],
          })

          topics_with_failures << message.topic
          failed_messages << message
        end
      end

      if failed_messages.any?
        failed_messages.group_by(&:topic).each do |topic, messages|
          @logger.error "Failed to assign partitions to #{messages.count} messages in #{topic}"
        end

        @cluster.mark_as_stale!
      end

      @pending_message_queue.replace(failed_messages)
    end

    def buffer_messages
      messages = []

      @pending_message_queue.each do |message|
        messages << message
      end

      @buffer.each do |topic, partition, messages_for_partition|
        messages_for_partition.each do |message|
          messages << PendingMessage.new(
            message.value,
            message.key,
            topic,
            partition,
            nil,
            message.create_time
          )
        end
      end

      messages
    end

    def buffer_overflow(topic, message)
      @instrumenter.instrument("buffer_overflow.producer", {
        topic: topic,
      })

      raise BufferOverflow, message
    end
  end
end
