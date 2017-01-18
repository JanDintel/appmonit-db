module Appmonit::DB
  struct CollectionIndex
    getter collection_name : String
    getter columns : Hash(String, ColumnIndex)

    delegate :[], to: columns

    def self.from_io(io)
      collection_index = CollectionIndex.new(io.gets(io.read_bytes(Int32)).to_s)
      io.read_bytes(Int32).times do
        column_index = ColumnIndex.from_io(io)
        collection_index.columns[column_index.column_name] = column_index
      end
      collection_index
    end

    def initialize(@collection_name)
      @columns = Hash(String, ColumnIndex).new { |h, k| h[k] = ColumnIndex.new(k) }
    end

    def to_io(io)
      io.write_bytes(collection_name.size)
      io.write(collection_name.to_slice)
      io.write_bytes(columns.size)
      columns.values.each(&.to_io(io))
    end
  end
end
