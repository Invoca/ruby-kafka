describe EbKafka::Client do
  it "accepts valid seed brokers URIs" do
    expect {
      EbKafka::Client.new(seed_brokers: ["eb-kafka://kafka"])
    }.not_to raise_exception

    expect {
      EbKafka::Client.new(seed_brokers: ["eb-kafka+ssl://kafka"])
    }.not_to raise_exception

    expect {
      EbKafka::Client.new(seed_brokers: ["http://kafka"])
    }.to raise_exception(EbKafka::Error, "invalid protocol `http` in `http://kafka`")
  end
end
