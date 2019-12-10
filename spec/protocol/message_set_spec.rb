describe EbKafka::Protocol::Message do
  include EbKafka::Protocol

  it "decodes message sets" do
    message1 = EbKafka::Protocol::Message.new(value: "hello")
    message2 = EbKafka::Protocol::Message.new(value: "good-day")

    message_set = EbKafka::Protocol::MessageSet.new(messages: [message1, message2])

    data = StringIO.new
    encoder = EbKafka::Protocol::Encoder.new(data)

    message_set.encode(encoder)

    data.rewind

    decoder = EbKafka::Protocol::Decoder.new(data)
    new_message_set = EbKafka::Protocol::MessageSet.decode(decoder)

    expect(new_message_set.messages).to eq [message1, message2]
  end

  it "skips the last message if it has been truncated" do
    message1 = EbKafka::Protocol::Message.new(value: "hello")
    message2 = EbKafka::Protocol::Message.new(value: "good-day")

    message_set = EbKafka::Protocol::MessageSet.new(messages: [message1, message2])

    data = StringIO.new
    encoder = EbKafka::Protocol::Encoder.new(data)

    message_set.encode(encoder)

    data.rewind
    data.truncate(data.size - 1)

    decoder = EbKafka::Protocol::Decoder.new(data)
    new_message_set = EbKafka::Protocol::MessageSet.decode(decoder)

    expect(new_message_set.messages).to eq [message1]
  end

  it "raises MessageTooLargeToRead if the first message in the set has been truncated" do
    message = EbKafka::Protocol::Message.new(value: "hello")

    message_set = EbKafka::Protocol::MessageSet.new(messages: [message])

    data = StringIO.new
    encoder = EbKafka::Protocol::Encoder.new(data)

    message_set.encode(encoder)

    data.rewind
    data.truncate(data.size - 1)

    decoder = EbKafka::Protocol::Decoder.new(data)

    expect {
      EbKafka::Protocol::MessageSet.decode(decoder)
    }.to raise_exception(EbKafka::MessageTooLargeToRead)
  end

  describe '.decode' do
    let(:instrumenter) { EbKafka::Instrumenter.new(client_id: "test") }
    let(:compressor) { EbKafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter) }

    def encode(messages: [], wrapper_message_offset: -1)
      message_set = EbKafka::Protocol::MessageSet.new(messages: messages)
      compressed_message_set = compressor.compress(message_set, offset: wrapper_message_offset)
      EbKafka::Protocol::Encoder.encode_with(compressed_message_set)
    end

    def decode(data)
      decoder = EbKafka::Protocol::Decoder.from_string(data)
      EbKafka::Protocol::MessageSet
        .decode(decoder)
        .messages
    end

    it "sets offsets correctly for compressed messages with relative offsets" do
      message1 = EbKafka::Protocol::Message.new(value: "hello1", offset: 0)
      message2 = EbKafka::Protocol::Message.new(value: "hello2", offset: 1)
      message3 = EbKafka::Protocol::Message.new(value: "hello3", offset: 2)

      data = encode(messages: [message1, message2, message3], wrapper_message_offset: 1000)
      messages = decode(data)

      expect(messages.map(&:offset)).to eq [998, 999, 1000]
    end

    it "sets offsets correctly for compressed messages with relative offsets on a compacted topic" do
      message1 = EbKafka::Protocol::Message.new(value: "hello1", offset: 0)
      message2 = EbKafka::Protocol::Message.new(value: "hello2", offset: 2)
      message3 = EbKafka::Protocol::Message.new(value: "hello3", offset: 3)

      data = encode(messages: [message1, message2, message3], wrapper_message_offset: 1000)
      messages = decode(data)

      expect(messages.map(&:offset)).to eq [997, 999, 1000]
    end

    it "keeps the predefined offsets for messages delivered in 0.9 format" do
      message1 = EbKafka::Protocol::Message.new(value: "hello1", offset: 997)
      message2 = EbKafka::Protocol::Message.new(value: "hello2", offset: 999)
      message3 = EbKafka::Protocol::Message.new(value: "hello3", offset: 1000)

      data = encode(messages: [message1, message2, message3], wrapper_message_offset: 1000)
      messages = decode(data)

      expect(messages.map(&:offset)).to eq [997, 999, 1000]
    end
  end
end
