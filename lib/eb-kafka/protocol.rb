module EbKafka

  # The protocol layer of the library.
  #
  # The EbKafka protocol (https://kafka.apache.org/protocol) defines a set of API
  # requests, each with a well-known numeric API key, as well as a set of error
  # codes with specific meanings.
  #
  # This module, and the classes contained in it, implement the client side of
  # the protocol.
  module Protocol
    # The replica id of non-brokers is always -1.
    REPLICA_ID = -1

    PRODUCE_API = 0
    FETCH_API = 1
    LIST_OFFSET_API = 2
    TOPIC_METADATA_API = 3
    OFFSET_COMMIT_API = 8
    OFFSET_FETCH_API = 9
    GROUP_COORDINATOR_API = 10
    JOIN_GROUP_API = 11
    HEARTBEAT_API = 12
    LEAVE_GROUP_API = 13
    SYNC_GROUP_API = 14
    SASL_HANDSHAKE_API = 17
    API_VERSIONS_API = 18
    CREATE_TOPICS_API = 19
    DELETE_TOPICS_API = 20
    DESCRIBE_CONFIGS_API = 32
    CREATE_PARTITIONS_API = 37

    # A mapping from numeric API keys to symbolic API names.
    APIS = {
      PRODUCE_API => :produce,
      FETCH_API => :fetch,
      LIST_OFFSET_API => :list_offset,
      TOPIC_METADATA_API => :topic_metadata,
      OFFSET_COMMIT_API => :offset_commit,
      OFFSET_FETCH_API => :offset_fetch,
      GROUP_COORDINATOR_API => :group_coordinator,
      JOIN_GROUP_API => :join_group,
      HEARTBEAT_API => :heartbeat,
      LEAVE_GROUP_API => :leave_group,
      SYNC_GROUP_API => :sync_group,
      SASL_HANDSHAKE_API => :sasl_handshake,
      API_VERSIONS_API => :api_versions,
      CREATE_TOPICS_API => :create_topics,
      DELETE_TOPICS_API => :delete_topics,
      DESCRIBE_CONFIGS_API => :describe_configs_api,
      CREATE_PARTITIONS_API => :create_partitions
    }

    # A mapping from numeric error codes to exception classes.
    ERRORS = {
      -1 => UnknownError,
       1 => OffsetOutOfRange,
       2 => CorruptMessage,
       3 => UnknownTopicOrPartition,
       4 => InvalidMessageSize,
       5 => LeaderNotAvailable,
       6 => NotLeaderForPartition,
       7 => RequestTimedOut,
       8 => BrokerNotAvailable,
       9 => ReplicaNotAvailable,
      10 => MessageSizeTooLarge,
      12 => OffsetMetadataTooLarge,
      15 => GroupCoordinatorNotAvailable,
      16 => NotCoordinatorForGroup,
      17 => InvalidTopic,
      18 => RecordListTooLarge,
      19 => NotEnoughReplicas,
      20 => NotEnoughReplicasAfterAppend,
      21 => InvalidRequiredAcks,
      22 => IllegalGeneration,
      25 => UnknownMemberId,
      26 => InvalidSessionTimeout,
      27 => RebalanceInProgress,
      28 => InvalidCommitOffsetSize,
      29 => TopicAuthorizationFailed,
      30 => GroupAuthorizationFailed,
      31 => ClusterAuthorizationFailed,
      32 => InvalidTimestamp,
      33 => UnsupportedSaslMechanism,
      34 => InvalidSaslState,
      35 => UnsupportedVersion,
      36 => TopicAlreadyExists,
      37 => InvalidPartitions,
      38 => InvalidReplicationFactor,
      39 => InvalidReplicaAssignment,
      40 => InvalidConfig,
      41 => NotController,
      42 => InvalidRequest
    }

    # A mapping from int to corresponding resource type in symbol.
    # https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/ResourceType.java
    RESOURCE_TYPE_UNKNOWN = 0
    RESOURCE_TYPE_ANY = 1
    RESOURCE_TYPE_TOPIC = 2
    RESOURCE_TYPE_GROUP = 3
    RESOURCE_TYPE_CLUSTER = 4
    RESOURCE_TYPE_TRANSACTIONAL_ID = 5
    RESOURCE_TYPE_DELEGATION_TOKEN = 6
    RESOURCE_TYPES = {
      RESOURCE_TYPE_UNKNOWN => :unknown,
      RESOURCE_TYPE_ANY => :any,
      RESOURCE_TYPE_TOPIC => :topic,
      RESOURCE_TYPE_GROUP => :group,
      RESOURCE_TYPE_CLUSTER => :cluster,
      RESOURCE_TYPE_TRANSACTIONAL_ID => :transactional_id,
      RESOURCE_TYPE_DELEGATION_TOKEN => :delegation_token,
    }

    # Handles an error code by either doing nothing (if there was no error) or
    # by raising an appropriate exception.
    #
    # @param error_code Integer
    # @raise [ProtocolError]
    # @return [nil]
    def self.handle_error(error_code, error_message = nil)
      if error_code == 0
        # No errors, yay!
      elsif error = ERRORS[error_code]
        raise error, error_message
      else
        raise UnknownError, "Unknown error with code #{error_code} #{error_message}"
      end
    end

    # Returns the symbolic name for an API key.
    #
    # @param api_key Integer
    # @return [Symbol]
    def self.api_name(api_key)
      APIS.fetch(api_key, :unknown)
    end
  end
end

require "eb-kafka/protocol/metadata_request"
require "eb-kafka/protocol/metadata_response"
require "eb-kafka/protocol/produce_request"
require "eb-kafka/protocol/produce_response"
require "eb-kafka/protocol/fetch_request"
require "eb-kafka/protocol/fetch_response"
require "eb-kafka/protocol/list_offset_request"
require "eb-kafka/protocol/list_offset_response"
require "eb-kafka/protocol/group_coordinator_request"
require "eb-kafka/protocol/group_coordinator_response"
require "eb-kafka/protocol/join_group_request"
require "eb-kafka/protocol/join_group_response"
require "eb-kafka/protocol/sync_group_request"
require "eb-kafka/protocol/sync_group_response"
require "eb-kafka/protocol/leave_group_request"
require "eb-kafka/protocol/leave_group_response"
require "eb-kafka/protocol/heartbeat_request"
require "eb-kafka/protocol/heartbeat_response"
require "eb-kafka/protocol/offset_fetch_request"
require "eb-kafka/protocol/offset_fetch_response"
require "eb-kafka/protocol/offset_commit_request"
require "eb-kafka/protocol/offset_commit_response"
require "eb-kafka/protocol/api_versions_request"
require "eb-kafka/protocol/api_versions_response"
require "eb-kafka/protocol/sasl_handshake_request"
require "eb-kafka/protocol/sasl_handshake_response"
require "eb-kafka/protocol/create_topics_request"
require "eb-kafka/protocol/create_topics_response"
require "eb-kafka/protocol/delete_topics_request"
require "eb-kafka/protocol/delete_topics_response"
require "eb-kafka/protocol/describe_configs_request"
require "eb-kafka/protocol/describe_configs_response"
require "eb-kafka/protocol/create_partitions_request"
require "eb-kafka/protocol/create_partitions_response"
