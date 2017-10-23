require "encoding/encoding"
require "encoding/float_encoding"
require "encoding/delta_encoding"
require "encoding/delta_encoding_64"
require "encoding/bit_stream"
require "snappy"
require "msgpack"
require "./value"

module Appmonit::DB
  module Encoding
    def self.encode(values : ValuesType)
      buffer = IO::Memory.new

      case values
      when Int64Values
        encoder = Int64Encoder.new
      when Float64Values
        encoder = Float64Encoder.new
      when BoolValues
        encoder = BoolEncoder.new
      when ArrayValues
        encoder = ArrayEncoder.new
      else
        encoder = StringEncoder.new
      end

      timestamp_encoder = Int64Encoder.new
      uuid_encoder = Int32Encoder.new

      values.each do |value|
        timestamp_encoder << value.epoch
        uuid_encoder << value.uuid
        encoder << value.value
      end

      timestamp_encoder.flush
      timestamp_encoder.to_io(buffer)

      uuid_encoder.flush
      uuid_encoder.to_io(buffer)

      encoder.flush
      encoder.to_io(buffer)

      buffer.to_slice
    end

    def self.decode(encoded : Bytes, type : EncodingType, size = 2000) : ValuesType
      buffer = IO::Memory.new(encoded)
      timestamps = Int64Decoder.decode(buffer)
      uuids = Int32Decoder.decode(buffer)

      result = Values.create(type, size)

      case type
      when EncodingType::Int64
        values = Int64Decoder.decode(buffer)
        values.each_with_index do |value, index|
          result << Int64Value.new(timestamps[index], uuids[index], value)
        end
      when EncodingType::Float64
        values = Float64Decoder.decode(buffer)
        values.each_with_index do |value, index|
          result << Float64Value.new(timestamps[index], uuids[index], value)
        end
      when EncodingType::Bool
        values = BoolDecoder.decode(buffer)
        values.each_with_index do |value, index|
          result << BoolValue.new(timestamps[index], uuids[index], value)
        end
      when EncodingType::String
        values = StringDecoder.decode(buffer)
        values.each_with_index do |value, index|
          result << StringValue.new(timestamps[index], uuids[index], value)
        end
      when EncodingType::Array
        values = ArrayDecoder.decode(buffer)
        values.each_with_index do |value, index|
          result << ArrayValue.new(timestamps[index], uuids[index], value)
        end
      else
        raise "invalid encoding type"
      end

      result
    end

    def self.iterate(encoded : Bytes, type : EncodingType) : Iterator
      case type
      when EncodingType::Int64
        Int64ValuesIterator.new(encoded)
      when EncodingType::Float64
        Float64ValuesIterator.new(encoded)
      when EncodingType::Bool
        BoolValuesIterator.new(encoded)
      when EncodingType::String
        StringValuesIterator.new(encoded)
      when EncodingType::Array
        ArrayValuesIterator.new(encoded)
      else
        raise "invalid encoding type"
      end
    end

    class Int64ValuesIterator
      include ::Iterator(Int64Value)

      @timestamps : Array(Int64)
      @uuids : Array(Int32)

      def initialize(encoded)
        buffer = IO::Memory.new(encoded)
        @timestamps = Int64Decoder.decode(buffer)
        @uuids = Int32Decoder.decode(buffer)
        @values = Int64Iterator.new(buffer)
        @pos = 0
      end

      def next
        return stop if @timestamps.empty?
        value = @values.next
        if value.is_a?(Int64)
          timestamp = @timestamps[@pos]
          uuid = @uuids[@pos]
          @pos += 1
          Int64Value.new(timestamp, uuid, value)
        else
          stop
        end
      end
    end

    class Float64ValuesIterator
      include ::Iterator(Float64Value)

      @timestamps : Array(Int64)
      @uuids : Array(Int32)

      def initialize(encoded)
        buffer = IO::Memory.new(encoded)
        @timestamps = Int64Decoder.decode(buffer)
        @uuids = Int32Decoder.decode(buffer)
        @values = Float64Iterator.new(buffer)
        @pos = 0
      end

      def next
        return stop if @timestamps.empty?
        value = @values.next
        if value.is_a?(Float64)
          timestamp = @timestamps[@pos]
          uuid = @uuids[@pos]
          @pos += 1
          Float64Value.new(timestamp, uuid, value)
        else
          stop
        end
      end
    end

    class BoolValuesIterator
      include ::Iterator(BoolValue)

      @timestamps : Array(Int64)
      @uuids : Array(Int32)

      def initialize(encoded)
        buffer = IO::Memory.new(encoded)
        @timestamps = Int64Decoder.decode(buffer)
        @uuids = Int32Decoder.decode(buffer)
        @values = BoolIterator.new(buffer)
        @pos = 0
      end

      def next
        return stop if @timestamps.empty?
        value = @values.next
        if value.is_a?(Bool)
          timestamp = @timestamps[@pos]
          uuid = @uuids[@pos]
          @pos += 1
          BoolValue.new(timestamp, uuid, value)
        else
          stop
        end
      end
    end

    class StringValuesIterator
      include ::Iterator(StringValue)

      @timestamps : Array(Int64)
      @uuids : Array(Int32)

      def initialize(encoded)
        buffer = IO::Memory.new(encoded)
        @timestamps = Int64Decoder.decode(buffer)
        @uuids = Int32Decoder.decode(buffer)
        @values = StringIterator.new(buffer)
        @pos = 0
      end

      def next
        return stop if @timestamps.empty?
        value = @values.next
        if value.is_a?(String)
          timestamp = @timestamps[@pos]
          uuid = @uuids[@pos]
          @pos += 1
          StringValue.new(timestamp, uuid, value)
        else
          stop
        end
      end
    end

    class ArrayValuesIterator
      include ::Iterator(ArrayValue)

      @timestamps : Array(Int64)
      @uuids : Array(Int32)

      def initialize(encoded)
        buffer = IO::Memory.new(encoded)
        @timestamps = Int64Decoder.decode(buffer)
        @uuids = Int32Decoder.decode(buffer)
        @values = ArrayIterator.new(buffer)
        @pos = 0
      end

      def next
        return stop if @timestamps.empty?
        value = @values.next
        if value.is_a?(Array(String))
          timestamp = @timestamps[@pos]
          uuid = @uuids[@pos]
          @pos += 1
          ArrayValue.new(timestamp, uuid, value)
        else
          stop
        end
      end
    end

    abstract class Encoder(T)
      abstract def <<(value : T)
      abstract def flush
      abstract def to_io(io : IO)

      def <<(value)
        raise "Cannot add value"
      end
    end

    abstract class Decoder(T)
    end

    class Int32Encoder < Encoder(Int32)
      def initialize
        @integer_encoder = DeltaEncoding::Encoder.new
      end

      def <<(value : T)
        @integer_encoder.write_integer(value)
      end

      def flush
        @integer_encoder.flush
      end

      def to_io(io)
        @integer_encoder.to_io(io)
      end
    end

    class Int64Encoder < Encoder(Int64)
      def initialize
        @integer_encoder = DeltaEncoding64::Encoder.new
      end

      def <<(value : T)
        @integer_encoder.write_integer(value)
      end

      def flush
        @integer_encoder.flush
      end

      def to_io(io)
        @integer_encoder.to_io(io)
      end
    end

    class Float64Encoder < Encoder(Float64)
      def initialize
        @float_buffer = IO::Memory.new
        @float_encoder = FloatEncoding::Encoder.new(@float_buffer)
      end

      def <<(value : T)
        @float_encoder.push(value)
      end

      def flush
        @float_encoder.finish
      end

      def to_io(io)
        @float_buffer.rewind
        IO.copy(@float_buffer, io)
      end
    end

    class BoolEncoder < Encoder(Bool)
      def initialize
        @bool_buffer = IO::Memory.new
        @count = 0
        @bool_encoder = BitStream.new(@bool_buffer, :write)
      end

      def <<(value : T)
        @count += 1
        @bool_encoder.write_bit(value)
      end

      def flush
        @bool_encoder.flush(false)
      end

      def to_io(io)
        @bool_buffer.rewind
        io.write_bytes(@count, IO::ByteFormat::LittleEndian)
        IO.copy(@bool_buffer, io)
      end
    end

    class StringEncoder < Encoder(String)
      def initialize
        @strings = [] of String
      end

      def <<(value : T)
        @strings << value
      end

      def flush
        # nothing
      end

      def to_io(io)
        encoded = Snappy.deflate(@strings.to_msgpack)
        io.write_bytes(encoded.size, IO::ByteFormat::LittleEndian)
        io.write encoded
      end
    end

    class ArrayEncoder < Encoder(Array(String))
      def initialize
        @strings = [] of Array(String)
      end

      def <<(value : T)
        @strings << value
      end

      def flush
        # nothing
      end

      def to_io(io)
        encoded = Snappy.deflate(@strings.to_msgpack)
        io.write_bytes(encoded.size, IO::ByteFormat::LittleEndian)
        io.write encoded
      end
    end

    class Int32Decoder < Decoder(Int32)
      def self.decode(buffer)
        decoder = DeltaEncoding::Decoder.new(buffer)
        decoder.values
      end
    end

    class Int32Iterator
      include Iterator(Int32)

      def initialize(buffer)
        @decoder = DeltaEncoding::Decoder.new(buffer)
      end

      def next
        if @decoder.all_read?
          stop
        else
          @decoder.read_integer
        end
      end
    end

    class Int64Decoder < Decoder(Int64)
      def self.decode(buffer)
        decoder = DeltaEncoding64::Decoder.new(buffer)
        decoder.values
      end
    end

    class Int64Iterator
      include Iterator(Int64)

      def initialize(buffer)
        @decoder = DeltaEncoding64::Decoder.new(buffer)
      end

      def next
        if @decoder.all_read?
          stop
        else
          @decoder.read_integer
        end
      end
    end

    class Float64Decoder < Decoder(Float64)
      def self.decode(buffer)
        decoder = FloatEncoding::Decoder.new(buffer)
        values = Array(Float64).new
        while decoder.next
          values << decoder.value
        end
        values
      end
    end

    class Float64Iterator
      include Iterator(Float64)

      def initialize(buffer)
        @decoder = FloatEncoding::Decoder.new(buffer)
      end

      def next
        if @decoder.next
          @decoder.value
        else
          stop
        end
      end
    end

    class BoolDecoder < Decoder(Bool)
      def self.decode(buffer)
        count = buffer.read_bytes(Int32, IO::ByteFormat::LittleEndian)
        bool_decoder = BitStream.new(buffer, :read)
        values = Array(Bool).new
        count.times do
          values << bool_decoder.read_bit
        end
        values
      end
    end

    class BoolIterator
      include Iterator(Bool)

      @count : Int32

      def initialize(buffer)
        @count = buffer.read_bytes(Int32, IO::ByteFormat::LittleEndian)
        @bool_decoder = BitStream.new(buffer, :read)
      end

      def next
        if @count > 0
          @count -= 1
          @bool_decoder.read_bit
        else
          stop
        end
      end
    end

    class StringDecoder < Decoder(String)
      def self.decode(buffer)
        encoded = Slice(UInt8).new(buffer.read_bytes(Int32, IO::ByteFormat::LittleEndian))
        buffer.read_fully(encoded)
        packed = Snappy.inflate(encoded)

        Array(String).from_msgpack(packed)
      end
    end

    class StringIterator
      include Iterator(String)

      @count : Int32

      def initialize(buffer)
        encoded = ::Slice(UInt8).new(buffer.read_bytes(Int32, IO::ByteFormat::LittleEndian))
        buffer.read_fully(encoded)
        packed = Snappy.inflate(encoded)
        @unpacker = MessagePack::Unpacker.new(packed)
        @count = @unpacker.read_array_size
      end

      def next
        if @count > 0
          @count -= 1
          @unpacker.read_string
        else
          stop
        end
      end
    end

    class ArrayDecoder < Decoder(Array(String))
      def self.decode(buffer)
        encoded = Slice(UInt8).new(buffer.read_bytes(Int32, IO::ByteFormat::LittleEndian))
        buffer.read_fully(encoded)
        packed = Snappy.inflate(encoded)

        Array(Array(String)).from_msgpack(packed)
      end
    end

    class ArrayIterator
      include Iterator(Array(String))

      @count : Int32

      def initialize(buffer)
        encoded = ::Slice(UInt8).new(buffer.read_bytes(Int32, IO::ByteFormat::LittleEndian))
        buffer.read_fully(encoded)
        packed = Snappy.inflate(encoded)
        @unpacker = MessagePack::Unpacker.new(packed)
        @count = @unpacker.read_array_size
      end

      def next
        if @count > 0
          @count -= 1
          Array(String).new(@unpacker.read_array_size) do
            @unpacker.read_string
          end
        else
          stop
        end
      end
    end
  end
end
