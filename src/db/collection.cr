module Appmonit::DB
  class Collection
    getter columns
    getter collection_name : String

    @wal_file : WALFile

    def self.open(location : String)
      collection = Collection.new(location)
      begin
        yield collection
      ensure
        collection.close
      end
    end

    def initialize(@location : String)
      if (match = COLLECTION_REGEX.match(@location))
        @collection_name = match["collection"]
      else
        raise InvalidFileName.new(@location)
      end
      @columns = Set(String).new
      @wal_file = next_wal_file
    end

    def write(column_name : String, values : Values)
      @columns << column_name
    end

    def next_wal_file
      WALFile.new(@location, last_wal_sequence + 1)
    end

    def last_wal_sequence
      if wal_files.size > 0
        wal_files.sort_by(&.sequence).last.sequence
      else
        0
      end
    end

    def close
    end

    private def wal_files
      Dir.glob(File.join(@location, "/*.wal")).map do |file_name|
        WALFile.new(file_name)
      end
    end
  end
end
