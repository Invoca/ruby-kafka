module EbKafka
  module Protocol
    class LeaveGroupResponse
      attr_reader :error_code

      def initialize(error_code:)
        @error_code = error_code
      end

      def self.decode(decoder)
        new(error_code: decoder.int16)
      end
    end
  end
end
