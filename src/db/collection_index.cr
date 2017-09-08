module Appmonit::DB
  struct CollectionIndex
    getter collection_id : Int64
    getter column_ids : Hash(Int64, ColumnIndex)

    delegate :[], to: column_ids

    def self.from_file(location)
      File.open(location, "r") do |io|
        Util.read_header(io)
        self.from_io(io)
      end
    end

    def to_file(location)
      tmp_location = location.gsub(".idx", ".tidx")
      File.open(tmp_location, "w") do |file|
        Util.write_header(file)
        to_io(file)
        file.flush
      end
      FileUtils.rm(location) if File.exists?(location)
      FileUtils.mv(tmp_location, location)
    end

    def self.from_io(io)
      collection_index = CollectionIndex.new(io.read_bytes(Int64))
      io.read_bytes(Int32).times do
        column_index = ColumnIndex.from_io(io)
        collection_index.column_ids[column_index.column_id] = column_index
      end
      collection_index
    end

    def initialize(@collection_id)
      @column_ids = Hash(Int64, ColumnIndex).new { |h, k| h[k] = ColumnIndex.new(k) }
    end

    def to_io(io)
      io.write_bytes(collection_id)
      io.write_bytes(column_ids.size)
      column_ids.values.each(&.to_io(io))
    end
  end
end
