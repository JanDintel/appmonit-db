module Appmonit::DB
  class ADBWriter
    getter adb_file : ADBFile
    getter file : File
    getter bytes_written : Int64
    getter collection_index : CollectionIndex
    getter append : Bool

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
      Dir.mkdir_p(File.dirname(@adb_file.location))
      Dir.mkdir_p(File.dirname(@adb_file.index_location))

      collection_index = CollectionIndex.from_file(@adb_file.index_location)
      if collection_index
        if File.exists?(@adb_file.location)
          @append = true
        else
          collection_index = CollectionIndex.new(@adb_file.collection_id)
          @append = false
        end
      else
        collection_index = CollectionIndex.new(@adb_file.collection_id)
        @append = false
      end
      @collection_index = collection_index

      @locked = false

      if @append
        @file = File.open(@adb_file.location, "a+")
        @file.seek(0, IO::Seek::End)
        @bytes_written = @file.pos
      else
        @file = File.open(@adb_file.location, "w+")
        Util.write_header(@file)
        @bytes_written = @file.pos
      end
    end

    def lock
      @file.flock_exclusive
      @locked = true
    end

    def unlock
      @file.flock_unlock
      @locked = false
    end

    def write(column_id : Int64, values : ValuesType)
      encoded = Encoding.encode(values)

      write_block(column_id, BlockStat.new(values), encoded)
    end

    def write_block(column_id : Int64, block_stat : BlockStat, block : Bytes)
      raise MissingLockException.new unless @locked

      checksum = CRC32.checksum(block)

      offset = file.pos
      file.write_bytes(checksum)
      file.write_bytes(block.size)
      file.write(block)
      file.flush

      @bytes_written = file.pos
      @collection_index.byte_size = @bytes_written
      block_stat.update(offset: offset)

      @collection_index.column_ids[column_id].block_stats << block_stat
    end

    def write_index
      @collection_index.to_file(@adb_file.index_location, @bytes_written)
    end

    def column_ids
      @column_ids
    end

    def close
      @file.flush
      @file.close
      write_index
    end
  end
end
