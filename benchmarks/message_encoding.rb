require "kafka"

ready "message serialization" do
  before do
    message = EbKafka::Protocol::Message.new(
      value: "hello",
      key: "world",
    )

    @io = StringIO.new
    encoder = EbKafka::Protocol::Encoder.new(@io)
    message.encode(encoder)

    @decoder = EbKafka::Protocol::Decoder.new(@io)
  end

  go "decoding" do
    @io.rewind
    EbKafka::Protocol::Message.decode(@decoder)
  end
end
