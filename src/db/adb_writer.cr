module Appmonit::DB
  class ADBWriter
    getter adb_file : ADBFile
    getter file : File
    getter bytes_written : Int32
    getter collection_index : CollectionIndex

    def self.open(location : String | ADBFile)
      writer = new(location)
      begin
        writer.lock
        yield writer
      ensure
        writer.unlock
        writer.close
      end
    end

    def self.new(location : String)
      new(ADBFile.new(location))
    end

    def initialize(@adb_file : ADBFile)
      raise DBExists.new(@adb_file.location) if File.exists?(@adb_file.location)

      Dir.mkdir_p(File.dirname(@adb_file.location))
      Dir.mkdir_p(File.dirname(@adb_file.index_location))

      @file = File.open(@adb_file.location, "w+")
      @collection_index = CollectionIndex.new(@adb_file.collection_name)
      @locked = false

      @bytes_written = write_header(@file)
    end

    def lock
      @file.flock_exclusive
      @locked = true
    end

    def unlock
      @file.flock_unlock
      @locked = false
    end

    def write(column_name : String, values : ValuesType)
      encoded = Encoding.encode(values)

      write_block(column_name, BlockStat.new(values), encoded)
    end

    def write_block(column_name : String, block_stat : BlockStat, block : Bytes)
      raise MissingLockException.new unless @locked

      checksum = Zlib.crc32(block)

      file.write_bytes(checksum)
      file.write(block)
      file.flush

      size = block.size + sizeof(UInt64)
      offset = @bytes_written
      @bytes_written += size

      block_stat.update(offset: offset, byte_size: size)

      @collection_index.columns[column_name].block_stats << block_stat
    end

    def write_index
      File.open(@adb_file.index_location, "w") do |file|
        write_header(file)
        @collection_index.to_io(file)
        file.flush
      end
    end

    private def write_header(io)
      io.write(FILE_HEADER)
      io.write_bytes(FILE_VERSION)
      io.flush
      FILE_HEADER.size + sizeof(Int32)
    end

    def columns
      @columns
    end

    def close
      @file.flush
      @file.close
      write_index
    end
  end
end
