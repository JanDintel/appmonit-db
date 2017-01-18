module Appmonit::DB
  class WALWriter
    getter location : String
    property max_wal_size : Int32
    getter! file : File

    def self.open(location : String)
      writer = new(location)
      begin
        yield writer
      ensure
        writer.close
      end
    end

    def initialize(@location : String, @last_version = 0)
      Dir.mkdir_p(@location)

      @wal_name = ""
      @last_operation = Time.now
      @max_wal_size = 2 * 1024 * 1024

      close_open_files
      create_wal
    end

    def write_values(column_name : String, values : Values)
      compressed = compress(column_name, values)

      # This may block
      file.flock_exclusive do
        file.write_bytes(compressed.size)
        file.write(compressed)
        file.flush

        if current_wal_full?
          create_wal
        else
          false
        end
      end
    end

    def close
      close_open_file(@wal_name)
    end

    private def close_current
      file.flush
      file.close
      close_open_file(@wal_name)
    end

    private def close_open_file(file_name)
      file.flush
      wal_file_name = @wal_name.gsub(".cur", ".wal")
      File.rename(file_name, wal_file_name) if File.exists?(file_name)
    end

    private def create_wal
      close_current if @file

      @last_version += 1
      @wal_name = File.join(@location, @last_version.to_s.rjust(32, '0') + ".cur")

      @file = File.new(@wal_name, "w+")

      # This raises when the wal file already exists
      file.flock_exclusive(false) do
        raise DBExists.new(@wal_name) if File::Stat.new(@wal_name).size > 0
        write_header(file)
      end
      @last_operation = Time.now
      true
    end

    private def close_open_files
      Dir.glob(File.join(@location, "*.cur")).each do |open_file|
        begin
          close_open_file(open_file)
        rescue e : Exception
          puts "Shutting down. Error closing open wal files"
          raise e
        end
      end
    end

    private def current_wal_full?
      file.pos >= @max_wal_size
    end

    private def compress(column_name, values)
      io = IO::Memory.new
      io.write_bytes(column_name.size)
      io.write(column_name.to_slice)
      values.to_io(io)
      Snappy.deflate(io.to_slice)
    end

    private def write_header(io)
      io.write(FILE_HEADER)
      io.write_bytes(FILE_VERSION)
      io.flush
      FILE_HEADER.size + sizeof(Int32)
    end
  end
end
