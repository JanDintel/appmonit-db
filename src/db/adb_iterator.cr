module Appmonit::DB
  class ADBIterator
    @readers : Array(ADBReader)

    def initialize(adb_files : Array(ADBFile))
      @readers = adb_files.map { |adb_file| ADBReader.new(adb_file) }
    end

    def iterate_blocks
      @readers.each do |reader|
        reader.each_block do |column_name, block_stat, block|
          yield column_name, block_stat, block
        end
      end
    end

    def iterate_values(block_size)
      column_names = @readers.flat_map(&.column_names).uniq

      column_names.each do |column_name|
        encoding_types = @readers.flat_map(&.encoding_types(column_name)).uniq
        encoding_types.each do |encoding_type|
          blocks = @readers.flat_map(&.blocks(column_name, encoding_type))

          times = Array(Time).new(blocks.size * 2)
          blocks.each do |block|
            times << block.min_time
            times << block.max_time + 1.second
          end

          values = Values.create(encoding_type, block_size)

          values_written = 0
          times.sort.each_cons(2) do |(min_time, max_time)|
            blocks.each do |block|
              if block.in_range(min_time, max_time)
                block[min_time, max_time].each do |value|
                  values << value
                  values_written += 1
                  if values_written == block_size
                    yield column_name, BlockStat.new(values), Encoding.encode(values.sort_by!(&.created_at))
                    values.clear
                    values_written = 0
                  end
                end
              end
            end
          end
          yield column_name, BlockStat.new(values), Encoding.encode(values.sort_by!(&.created_at)) if values_written > 0
        end
      end
    end
  end
end
