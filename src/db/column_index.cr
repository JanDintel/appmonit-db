module Appmonit::DB
  struct ColumnIndex
    getter column_id : Int64

    def self.from_io(io)
      column_index = ColumnIndex.new(io.read_bytes(Int64))
      io.read_bytes(Int32).times do
        column_index.block_stats << BlockStat.from_io(io)
      end
      column_index
    end

    def initialize(@column_id)
      @block_stats = Array(BlockStat).new
    end

    def block_stats(encoding_type = nil)
      if encoding_type
        @block_stats.select { |block_stat| block_stat.encoding_type == encoding_type }
      else
        @block_stats
      end
    end

    def encoding_types
      encoding_types = Set(EncodingType).new(5)
      @block_stats.each do |block_stat|
        encoding_types << block_stat.encoding_type
        break if encoding_types.size == 5 # we already found them all
      end
      encoding_types.to_a
    end

    def to_io(io)
      io.write_bytes(column_id)
      io.write_bytes(block_stats.size)
      block_stats.each(&.to_io(io))
    end
  end
end
