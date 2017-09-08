module Appmonit::DB
  class ADBReader
    getter adb_file : ADBFile
    getter file : File
    getter collection_index : CollectionIndex

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
      Util.read_header(@file)
      @collection_index = CollectionIndex.from_file(@adb_file.index_location)
    end

    def column_ids : Array(Int64)
      @collection_index.column_ids.keys.sort
    end

    def read_block(offset)
      @file.seek(offset, IO::Seek::Set)

      checksum = @file.read_bytes(UInt32)
      size = @file.read_bytes(Int32)
      buffer = Slice(UInt8).new(size)
      @file.read_fully(buffer)

      raise ChecksumFailed.new if CRC32.checksum(buffer) != checksum

      buffer
    end

    def read_values(block_stat : BlockStat, min_time : Time, max_time : Time) : Values
      encoded = read_block(block_stat.offset)
      values = Values.new
      Encoding.decode(encoded, block_stat.encoding_type).each do |value|
        values << value if value.created_at >= min_time && value.created_at < max_time
      end
      values
    end

    def read_values(column_id, min_time : Time = Time::MinValue, max_time : Time = Time::MaxValue) : Values
      block_stats = @collection_index.column_ids[column_id].block_stats
      selected = block_stats.select(&.in_range(min_time, max_time))

      values = Values.new
      selected.each do |block_stat|
        values.concat(read_values(block_stat, min_time, max_time))
      end
      values
    end

    def close
      @file.close
    end
  end
end
