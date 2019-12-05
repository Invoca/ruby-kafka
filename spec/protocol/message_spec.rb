describe EbKafka::Protocol::Message do
  it "encodes and decodes messages" do
    message = EbKafka::Protocol::Message.new(
      value: "yolo",
      key: "xx",
    )

    io = StringIO.new
    encoder = EbKafka::Protocol::Encoder.new(io)
    message.encode(encoder)
    data = StringIO.new(io.string)
    decoder = EbKafka::Protocol::Decoder.new(data)

    expect(EbKafka::Protocol::Message.decode(decoder)).to eq message
  end

  it "decodes messages written in the 0.9 format" do
    data = File.open("spec/fixtures/message-0.9-format")

    decoder = EbKafka::Protocol::Decoder.new(data)
    message = EbKafka::Protocol::Message.decode(decoder)

    expect(message.key).to eq "xx"
    expect(message.value).to eq "yolo"

    # Messages didn't have timestamps back in the 0.9 days.
    expect(message.create_time).to eq nil
  end
end
