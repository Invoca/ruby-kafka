require "eb-kafka/datadog"
require "fake_datadog_agent"

describe EbKafka::Datadog do
  let(:agent) { FakeDatadogAgent.new }

  before do
    agent.start
  end

  after do
    agent.stop
  end

  it "emits metrics to the Datadog agent" do
    EbKafka::Datadog.host = agent.host
    EbKafka::Datadog.port = agent.port

    client = EbKafka::Datadog.statsd

    client.increment("greetings")

    agent.wait_for_metrics

    expect(agent.metrics.count).to eq 1

    metric = agent.metrics.first

    expect(metric).to eq "ruby_kafka.greetings"
  end
end
