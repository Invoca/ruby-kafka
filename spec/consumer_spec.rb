describe Kafka::Consumer do
  let(:cluster) { double(:cluster) }
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test", group_id: "test") }
  let(:group) { double(:group) }
  let(:offset_manager) { double(:offset_manager) }
  let(:heartbeat) { double(:heartbeat) }
  let(:fetch_operation) { double(:fetch_operation) }
  let(:session_timeout) { 30 }
  let(:assigned_partitions) { { "greetings" => [0] } }

  let(:consumer) {
    Kafka::Consumer.new(
      cluster: cluster,
      logger: logger,
      instrumenter: instrumenter,
      group: group,
      offset_manager: offset_manager,
      session_timeout: session_timeout,
      heartbeat: heartbeat,
    )
  }

  before do
    allow(Kafka::FetchOperation).to receive(:new) { fetch_operation }

    allow(cluster).to receive(:add_target_topics)
    allow(cluster).to receive(:refresh_metadata_if_necessary!)

    allow(offset_manager).to receive(:commit_offsets)
    allow(offset_manager).to receive(:set_default_offset)
    allow(offset_manager).to receive(:next_offset_for) { 42 }

    allow(group).to receive(:subscribe)
    allow(group).to receive(:leave)
    allow(group).to receive(:member?) { true }
    allow(group).to receive(:assigned_partitions) { assigned_partitions }

    allow(heartbeat).to receive(:send_if_necessary)

    allow(fetch_operation).to receive(:fetch_from_partition)
    allow(fetch_operation).to receive(:execute) { fetched_batches }

    consumer.subscribe("greetings")
  end

  describe "#each_message" do
    let(:messages) {
      [
        Kafka::FetchedMessage.new(
          value: "hello",
          key: nil,
          topic: "greetings",
          partition: 0,
          offset: 13,
        )
      ]
    }

    let(:fetched_batches) {
      [
        Kafka::FetchedBatch.new(
          topic: "greetings",
          partition: 0,
          highwater_mark_offset: 42,
          messages: messages,
        )
      ]
    }

    it "logs exceptions raised in the block" do
      expect {
        consumer.each_message do |message|
          raise "hello"
        end
      }.to raise_exception(RuntimeError, "hello")

      expect(log.string).to include "Exception raised when processing greetings/0 at offset 13 -- RuntimeError: hello"
    end
  end
end