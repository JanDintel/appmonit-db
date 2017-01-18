require "../spec_helper"

module Appmonit::DB
  describe ADBCompactor do
    it "compacts multiple adb files into 1 adb file" do
      values1 = Int64Values{
        Value[Time.epoch(0), 100, 1],
        Value[Time.epoch(1), 101, 1],
        Value[Time.epoch(2), 102, 1],
      }
      encoded = Encoding.encode(values1)

      ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0000001.adb") do |writer|
        writer.write_block("my-column", BlockStat.new(values1), encoded)
      end

      values2 = Int64Values{
        Value[Time.epoch(3), 100, 1],
        Value[Time.epoch(4), 101, 1],
        Value[Time.epoch(5), 102, 1],
      }
      encoded = Encoding.encode(values2)

      ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0000002.adb") do |writer|
        writer.write_block("my-column", BlockStat.new(values2), encoded)
      end

      shard = Shard.new("/tmp/appmonit-db/my-collection/0-60/")

      ADBCompactor.new(shard).compact

      ADBReader.open("/tmp/appmonit-db/my-collection/0-60/2-0000003.adb") do |reader|
        reader.read_values("my-column").sort_by(&.created_at).should eq values1 + values2
      end
    end

    it "compacts overlapping adb files" do
      values1 = Int64Values{
        Value[Time.epoch(0), 100, 1],
        Value[Time.epoch(1), 101, 1],
        Value[Time.epoch(3), 102, 1],
      }
      encoded = Encoding.encode(values1)

      ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0000001.adb") do |writer|
        writer.write_block("my-column", BlockStat.new(values1), encoded)
      end

      values2 = Int64Values{
        Value[Time.epoch(2), 100, 1],
        Value[Time.epoch(4), 101, 1],
        Value[Time.epoch(5), 102, 1],
      }
      encoded = Encoding.encode(values2)

      ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0000002.adb") do |writer|
        writer.write_block("my-column", BlockStat.new(values2), encoded)
      end

      shard = Shard.new("/tmp/appmonit-db/my-collection/0-60/")

      ADBCompactor.new(shard).compact(true)

      ADBReader.open("/tmp/appmonit-db/my-collection/0-60/2-0000003.adb") do |reader|
        reader.read_values("my-column").should eq (values1 + values2).sort_by!(&.created_at)
      end
    end
  end
end
