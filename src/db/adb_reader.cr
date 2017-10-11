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
      @collection_index = CollectionIndex.from_file!(@adb_file.index_location)
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

    def iterate(column_id, min_time, max_time)
      ColumnIterator.new(self, @collection_index.map_block_stats(column_id, min_time, max_time), min_time, max_time)
    end

    class ColumnIterator
      include Iterator(Value)

      @iterators : Array(Iterator(Value))
      @next_values : Hash(Int32, Value)

      def initialize(@adb_reader : ADBReader, @block_stats : Array(BlockStat), @min_time : Time, @max_time : Time)
        @iterators = [] of Iterator(Value)
        @next_values = {} of Int32 => Value
      end

      def next
        load_values

        if @next_values.any?
          index, value = @next_values.min_by { |index, value| {value.created_at, value.uuid} }
          @next_values.delete(index)
        else
          stop
        end
      end

      private def load_values
        if @iterators.empty?
          load_iterators
        end

        @iterators.each_with_index do |iterator, index|
          unless @next_values[index]?
            iterator.each do |value|
              if value.created_at >= @min_time && value.created_at <= @max_time
                @next_values[index] = value
                break
              end
            end
          end
        end
      end

      private def load_iterators
        return if @block_stats.empty?

        current_block = @block_stats.shift
        buffer = @adb_reader.read_block(current_block.offset)
        @iterators << Encoding.iterate(buffer, current_block.encoding_type)
        while @block_stats.any? && current_block.overlap?(@block_stats.first)
          current_block = @block_stats.shift

          @iterators << Encoding.iterate(@adb_reader.read_block(current_block.offset), current_block.encoding_type)
        end
      end
    end
  end
end
