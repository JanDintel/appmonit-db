require "./value"

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

    def read_values(block_stat : BlockStat, min_epoch : Int64, max_epoch : Int64) : Values
      encoded = read_block(block_stat.offset)
      values = Values.new
      Encoding.decode(encoded, block_stat.encoding_type).each do |value|
        values << value if value.epoch >= min_epoch && value.epoch < max_epoch
      end
      values
    end

    def read_values(column_id, min_epoch : Int64 = Int64::MIN, max_epoch : Int64 = Int64::MAX) : Values
      block_stats = @collection_index.column_ids[column_id].block_stats
      selected = block_stats.select(&.in_range(min_epoch, max_epoch))

      values = Values.new
      selected.each do |block_stat|
        values.concat(read_values(block_stat, min_epoch, max_epoch))
      end
      values
    end

    def close
      @file.close
    end

    def iterate(column_ids : Array(Int64), min_epoch, max_epoch)
      RowIterator.new(self, column_ids, min_epoch, max_epoch)
    end

    def iterate(column_id, min_epoch, max_epoch)
      ColumnIterator.new(self, @collection_index.map_block_stats(column_id, min_epoch, max_epoch), min_epoch, max_epoch)
    end

    class RowIterator
      include Iterator(Array(Value?))

      @iterators : Array(ADBReader::ColumnIterator)
      @next_values : Hash(Int32, Value)
      @row_id : Tuple(Int64, Int32)
      @num_columns : Int32

      def initialize(@adb_reader : ADBReader, @column_ids : Array(Int64), @min_epoch : Int64, @max_epoch : Int64)
        @next_values = {} of Int32 => Value

        @row_id = {Int64::MIN, Int32::MIN}
        @num_columns = @column_ids.size
        @current_row = Array(Value?).new(@num_columns)

        @iterators = @column_ids.map { |id| @adb_reader.iterate(id, @min_epoch, @max_epoch) }
      end

      def next
        load_values
        @current_row.clear

        if @next_values.any?
          index, value = @next_values.min_by { |index, value| value.row_id }

          if value.row_id > @row_id
            @row_id = value.row_id
          end

          @num_columns.times do |index|
            value = @next_values[index]?
            if value && value.row_id == @row_id
              @current_row << value
              @next_values.delete(index)
            else
              @current_row << nil
            end
          end
          @current_row
        else
          stop
        end
      end

      private def load_values
        @num_columns.times do |index|
          iterator = @iterators[index]
          unless @next_values[index]?
            loop do
              value = iterator.next
              if value.is_a?(DB::Value)
                if value.row_id > @row_id
                  @next_values[index] = value
                  break
                end
              else
                break
              end
            end
          end
        end
      end
    end

    class ColumnIterator
      include Iterator(Value)

      def initialize(@adb_reader : ADBReader, @block_stats : Array(BlockStat), @min_epoch : Int64, @max_epoch : Int64)
        @current_values = Array(DB::Value).new(2000)
        @values = Array(DB::Value).new(5000)
      end

      def next
        if @current_values.empty?
          load_values
        end

        if @current_values.any?
          @current_values.pop
        else
          stop
        end
      end

      private def load_values
        if @block_stats.empty?
          @current_values = @values.sort! { |a, b| b.row_id <=> a.row_id }
          @values.clear
          return
        end

        current_block = @block_stats.pop

        @values.reject! do |value|
          if value.epoch >= @min_epoch && value.epoch < @max_epoch
            @current_values << value
            true
          end
        end

        DB::Encoding.iterate(@adb_reader.read_block(current_block.offset), current_block.encoding_type).each do |value|
          if value.epoch >= @min_epoch && value.epoch < @max_epoch
            @current_values << value
          else
            @values << value
          end
        end

        while @block_stats.any? && current_block.overlap?(@block_stats.last)
          next_block = @block_stats.pop

          DB::Encoding.iterate(@adb_reader.read_block(next_block.offset), next_block.encoding_type).each do |value|
            if value.epoch >= @min_epoch && value.epoch < @max_epoch
              if value.epoch <= current_block.max_epoch
                @current_values << value
              else
                @values << value
              end
            end
          end
        end

        @current_values.sort! { |a, b| b.row_id <=> a.row_id }
      end
    end
  end
end
