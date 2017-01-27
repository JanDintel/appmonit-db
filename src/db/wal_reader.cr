module Appmonit::DB
  class WALReader
    getter location : String

    def self.open(location : String)
      reader = new(location)
      begin
        yield reader
      ensure
        reader.close
      end
    end

    def initialize(@location : String)
      @values = Hash(String, Values).new { |h, k| h[k] = Values.new }

      load_files
    end

    def read_values(column_name, min_time, max_time)
      @values[column_name].select do |value|
        value.created_at >= min_time && value.created_at < max_time
      end
    end

    def close
      # nothing to do for now
    end

    private def load_file(file_name)
      File.open(file_name, "r") do |file|
        read_header(file)
        until file.pos == file.size
          file.read_fully(encoded = Bytes.new(file.read_bytes(Int32)))
          io = IO::Memory.new(Snappy.inflate(encoded))

          column_name = io.gets(io.read_bytes(Int32)).to_s
          @values[column_name].concat(Values.from_io(io))
        end
      end
    end

    private def load_files
      Dir.glob(File.join(@location, "*.wal")).each do |file_name|
        load_file(file_name)
      end
    end

    private def read_header(io)
      io.read_fully(header = Bytes.new(FILE_HEADER.size))
      unless io.read_bytes(Int32) == FILE_VERSION && header == FILE_HEADER
        raise InvalidHeader.new
      end
    end
  end
end
