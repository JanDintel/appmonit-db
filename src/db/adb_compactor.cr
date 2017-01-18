module Appmonit::DB
  class ADBCompactor
    def initialize(@shard : Shard)
    end

    def compact(full = false)
      level = 2
      adb_files = @shard.files_for_level(level)
      new_adb_file = adb_files.first.advance(level, @shard.last_adb_sequence + 1)

      if full
        full_compact(adb_files, new_adb_file)
      else
        fast_compact(adb_files, new_adb_file)
      end
    end

    private def fast_compact(adb_files, new_adb_file)
      ADBWriter.open(new_adb_file) do |writer|
        readers = ADBIterator.new(adb_files)

        readers.iterate_blocks do |column_name, block_stat, block|
          writer.write_block(column_name, block_stat, block)
        end
      end
    end

    private def full_compact(adb_files, new_adb_file)
      ADBWriter.open(new_adb_file) do |writer|
        readers = ADBIterator.new(adb_files)

        readers.iterate_values(2000) do |column_name, block_stat, block|
          writer.write_block(column_name, block_stat, block)
        end
      end
    end
  end
end
