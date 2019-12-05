describe EbKafka::Compressor do
  describe ".compress" do
    let(:instrumenter) { EbKafka::Instrumenter.new(client_id: "test") }

    it "only compresses the messages if there are at least the configured threshold" do
      compressor = EbKafka::Compressor.new(codec_name: :snappy, threshold: 3, instrumenter: instrumenter)

      message1 = EbKafka::Protocol::Message.new(value: "hello1")
      message2 = EbKafka::Protocol::Message.new(value: "hello2")

      message_set = EbKafka::Protocol::MessageSet.new(messages: [message1, message2])
      compressed_message_set = compressor.compress(message_set)

      expect(compressed_message_set.messages).to eq [message1, message2]
    end

    it "reduces the data size" do
      compressor = EbKafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter)

      message1 = EbKafka::Protocol::Message.new(value: "hello1" * 100)
      message2 = EbKafka::Protocol::Message.new(value: "hello2" * 100)

      message_set = EbKafka::Protocol::MessageSet.new(messages: [message1, message2])
      compressed_message_set = compressor.compress(message_set)

      uncompressed_data = EbKafka::Protocol::Encoder.encode_with(message_set)
      compressed_data = EbKafka::Protocol::Encoder.encode_with(compressed_message_set)

      expect(compressed_data.bytesize).to be < uncompressed_data.bytesize
    end
  end
end
