module Appmonit::DB
  class ADBReader
    getter adb_file : ADBFile
    getter file : File
    getter collection_index : CollectionIndex

    class Block
      getter reader : ADBReader
      getter block_stat : BlockStat
      getter loaded : Bool

      delegate min_time, max_time, in_range, to: block_stat

      def initialize(@reader, @block_stat)
        @values = Values.new(@block_stat.size)
        @loaded = false
      end

      def load
        return if @loaded

        encoded = @reader.read_block(@block_stat.offset, @block_stat.byte_size)
        Encoding.decode(encoded, @block_stat.encoding_type).each do |value|
          @values << value
        end
        @loaded = true
      end

      def [](min_time, max_time)
        load unless loaded
        @values.select { |value| value.created_at >= min_time && value.created_at < max_time }
      end
    end

    def self.open(location : String)
      reader = new(location)
      begin
        yield reader
      ensure
        reader.close
      end
    end

    def self.new(location : String)
      new(ADBFile.new(location))
    end

    def initialize(@adb_file : ADBFile)
      raise DBMissing.new(@adb_file.location) unless File.exists?(@adb_file.location)
      raise DBMissing.new(@adb_file.index_location) unless File.exists?(@adb_file.index_location)

      @file = File.open(@adb_file.location, "r")
      @locked = false

      read_header(@file)

      @collection_index = File.open(@adb_file.index_location, "r") do |index|
        read_header(index)
        CollectionIndex.from_io(index)
      end
    end

    def block_stats(column_name, encoding_type)
      @collection_index[column_name].block_stats(encoding_type)
    end

    def blocks(column_name, encoding_type)
      @collection_index[column_name].block_stats(encoding_type).map do |block_stat|
        Block.new(self, block_stat)
      end
    end

    def encoding_types(column_name)
      @collection_index[column_name].encoding_types
    end

    def column_names : Array(String)
      @collection_index.columns.keys.sort
    end

    def each_block
      @collection_index.columns.each do |column_name, column_index|
        column_index.block_stats.each do |block_stat|
          yield column_name, block_stat, read_block(block_stat.offset, block_stat.byte_size)
        end
      end
    end

    def read_block(offset, size)
      @file.seek(offset, IO::Seek::Set)

      checksum = @file.read_bytes(Int64)
      buffer = Slice(UInt8).new(size - sizeof(Int64))
      @file.read_fully(buffer)

      raise ChecksumFailed.new if Zlib.crc32(buffer) != checksum

      buffer
    end

    def read_values(block_stat : BlockStat, min_time : Time, max_time : Time) : Values
      encoded = read_block(block_stat.offset, block_stat.byte_size)
      values = Values.new
      Encoding.decode(encoded, block_stat.encoding_type).each do |value|
        values << value if value.created_at >= min_time && value.created_at < max_time
      end
      values
    end

    def read_values(column_name, min_time : Time = Time::MinValue, max_time : Time = Time::MaxValue) : Values
      block_stats = @collection_index.columns[column_name].block_stats
      selected = block_stats.select(&.in_range(min_time, max_time))

      values = Values.new
      selected.each do |block_stat|
        values += read_values(block_stat, min_time, max_time)
      end
      values
    end

    def read_values(column_name, encoding_type : EncodingType, min_time : Time = Time::MinValue, max_time : Time = Time::MaxValue)
      block_stats = @collection_index.columns[column_name].block_stats
      selected = block_stats.select do |block_stat|
        block_stat.in_range(min_time, max_time) &&
          encoding_type.includes?(block_stat.encoding_type)
      end

      values = Values.new
      selected.each do |block_stat|
        values += read_values(block_stat, min_time, max_time)
      end
      values
    end

    def close
      @file.close
    end

    def read_header(io)
      io.seek(0, IO::Seek::Set)
      io.read_fully(header = Bytes.new(10))
      version = io.read_bytes(Int32)
      if header != FILE_HEADER
        raise InvalidHeader.new
      elsif version != FILE_VERSION
        raise InvalidVersion.new("Expected #{FILE_VERSION} got #{version}")
      end
    end
  end
end
