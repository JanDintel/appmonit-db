require "../spec_helper"

module Appmonit::DB
  describe ADBIterator do
    context "iterate_blocks" do
      it "yields a block and block stat for with a column" do
        values1 = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }
        encoded = Encoding.encode(values1)

        ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0000001.adb") do |writer|
          writer.write_block("my-column", BlockStat.new(values1), encoded)
        end

        ADBIterator.new([ADBFile.new("/tmp/appmonit-db/my-collection/0-60/1-0000001.adb")]).iterate_blocks do |column_name, block_stat, block|
          column_name.should eq "my-column"
          block_stat.should be_a(BlockStat)
          block.should eq encoded
        end
      end
    end

    it "yields a block and block stat for with a column" do
      values1 = Int64Values{
        Value[Time.epoch(0), 100, 1],
        Value[Time.epoch(1), 101, 1],
        Value[Time.epoch(2), 102, 1],
        Value[Time.epoch(3), 103, 1],
      }
      encoded1 = Encoding.encode(values1)
      values2 = Int64Values{
        Value[Time.epoch(2), 104, 1],
        Value[Time.epoch(3), 105, 1],
        Value[Time.epoch(4), 106, 1],
        Value[Time.epoch(5), 107, 1],
      }
      encoded2 = Encoding.encode(values2)

      ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0000001.adb") do |writer|
        writer.write_block("my-column", BlockStat.new(values1), encoded1)
        writer.write_block("my-column", BlockStat.new(values2), encoded2)
      end

      ADBIterator.new([ADBFile.new("/tmp/appmonit-db/my-collection/0-60/1-0000001.adb")]).iterate_values(10) do |column_name, block_stat, block|
        column_name.should eq "my-column"
        block_stat.should be_a(BlockStat)
        Encoding.decode(block, EncodingType::Int64).should eq (values1 + values2).sort_by(&.created_at)
      end
    end
  end
end
