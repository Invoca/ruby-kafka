describe EbKafka::Cluster do
  describe "#get_leader" do
    let(:broker) { double(:broker) }
    let(:broker_pool) { double(:broker_pool) }

    let(:cluster) {
      EbKafka::Cluster.new(
        seed_brokers: [URI("kafka://test1:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
      )
    }

    before do
      allow(broker_pool).to receive(:connect) { broker }
      allow(broker).to receive(:disconnect)
    end

    it "raises LeaderNotAvailable if there's no leader for the partition" do
      metadata = EbKafka::Protocol::MetadataResponse.new(
        brokers: [
          EbKafka::Protocol::MetadataResponse::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
        topics: [
          EbKafka::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            partitions: [
              EbKafka::Protocol::MetadataResponse::PartitionMetadata.new(
                partition_id: 42,
                leader: 2,
                partition_error_code: 5, # <-- this is the important bit.
              )
            ]
          )
        ],
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error EbKafka::LeaderNotAvailable
    end

    it "raises InvalidTopic if the topic is invalid" do
      metadata = EbKafka::Protocol::MetadataResponse.new(
        brokers: [
          EbKafka::Protocol::MetadataResponse::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
        topics: [
          EbKafka::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            topic_error_code: 17, # <-- this is the important bit.
            partitions: []
          )
        ],
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error EbKafka::InvalidTopic
    end

    it "raises ConnectionError if unable to connect to any of the seed brokers" do
      cluster = EbKafka::Cluster.new(
        seed_brokers: [URI("kafka://not-there:9092"), URI("kafka://not-here:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
      )

      allow(broker_pool).to receive(:connect).and_raise(EbKafka::ConnectionError)

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_exception(EbKafka::ConnectionError)
    end
  end
end
