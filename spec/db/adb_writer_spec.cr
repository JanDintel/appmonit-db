require "../spec_helper"

module Appmonit::DB
  describe ADBWriter do
    it "writes the header when opened" do
      ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") { }
      file = File.open("/tmp/appmonit-db/1/0-60.adb")
      file.read(name = Bytes.new(FILE_HEADER.size))
      version = file.read_bytes(Int32)

      name.should eq FILE_HEADER
      version.should eq FILE_VERSION
    end

    it "writes the index when closed" do
      values = Int64Values{
        Value[0_i64, 100, 1],
        Value[1_i64, 101, 1],
        Value[2_i64, 102, 1],
      }

      collection_index = nil
      ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
        writer.write(1_i64, values)
        collection_index = writer.collection_index
      end

      index = File.open("/tmp/appmonit-db/1/0-60.idx")
      index.read(name = Bytes.new(FILE_HEADER.size))
      version = index.read_bytes(Int32)

      name.should eq FILE_HEADER
      version.should eq FILE_VERSION
      CollectionIndex.from_io(index).should eq collection_index
    end

    context "write values" do
      it "write the checksum and the block" do
        values = Int64Values{
          Value[0_i64, 100, 1],
          Value[1_i64, 101, 1],
          Value[2_i64, 102, 1],
        }

        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.write(1_i64, values)
        end

        encoded = Encoding.encode(values)

        file = File.open("/tmp/appmonit-db/1/0-60.adb")
        file.seek(14, IO::Seek::Set)

        checksum = file.read_bytes(UInt32)
        size = file.read_bytes(Int32)
        buffer = Slice(UInt8).new(size)
        file.read_fully(buffer)
        CRC32.checksum(encoded).should eq checksum
        buffer.should eq encoded
      end
    end

    context "write block" do
      it "writes a block to the file" do
        values = Int64Values{
          Value[0_i64, 100, 1],
          Value[1_i64, 101, 1],
          Value[2_i64, 102, 1],
        }
        encoded = Encoding.encode(values)

        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.write_block(1_i64, BlockStat.new(values), encoded)
        end

        file = File.open("/tmp/appmonit-db/1/0-60.adb")
        file.seek(14, IO::Seek::Set)

        checksum = file.read_bytes(UInt32)
        size = file.read_bytes(Int32)
        buffer = Slice(UInt8).new(size)
        file.read_fully(buffer)
        CRC32.checksum(encoded).should eq checksum
        buffer.should eq encoded
      end

      it "raises when the file is not locked" do
        values = Int64Values{
          Value[0_i64, 100, 1],
          Value[1_i64, 101, 1],
          Value[2_i64, 102, 1],
        }
        encoded = Encoding.encode(values)

        writer = ADBWriter.new("/tmp/appmonit-db/1/0-60.adb")
        expect_raises MissingLockException do
          writer.write_block(1_i64, BlockStat.new(values), encoded)
        end
      end
    end

    context "existing file" do
      it "marks the writer as appending if the file exsists" do
        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.append.should eq false
        end
        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.append.should eq true
        end
      end

      it "only writes the header once" do
        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") { }
        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") { }

        file = File.open("/tmp/appmonit-db/1/0-60.adb")
        file.read(name = Bytes.new(FILE_HEADER.size))
        version = file.read_bytes(Int32)

        name.should eq FILE_HEADER
        version.should eq FILE_VERSION

        file.read(name = Bytes.new(FILE_HEADER.size))
        name.should_not eq FILE_HEADER
      end

      it "sets the position to the end of the file" do
        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") { }
        File.open("/tmp/appmonit-db/1/0-60.adb", "a+") { |file| file << "bogus" }
        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.bytes_written.should eq 14 + "bogus".size
        end
      end

      it "appends a new block at the last known location" do
        values = Int64Values{
          Value[0_i64, 100, 1],
          Value[1_i64, 101, 1],
          Value[2_i64, 102, 1],
        }
        encoded = Encoding.encode(values)

        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.write_block(1_i64, BlockStat.new(values), encoded)
        end

        CollectionIndex.from_file!("/tmp/appmonit-db/1/0-60.idx").column_ids[1_i64].block_stats.size.should eq 1

        File.open("/tmp/appmonit-db/1/0-60.adb", "a+") { |file| file << "bogus" }

        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.write_block(1_i64, BlockStat.new(values), encoded)
        end

        CollectionIndex.from_file!("/tmp/appmonit-db/1/0-60.idx").map_block_stats(1_i64, 0_i64, 2_i64).size.should eq 2

        ADBReader.open("/tmp/appmonit-db/1/0-60.adb") do |reader|
          values = reader.read_values(1_i64, 0_i64, 3_i64)
          values.should be_a(Values)
          values.should eq Int64Values{
            Value[0_i64, 100, 1],
            Value[1_i64, 101, 1],
            Value[2_i64, 102, 1],
            Value[0_i64, 100, 1],
            Value[1_i64, 101, 1],
            Value[2_i64, 102, 1],
          }
        end
      end
    end

    it "creates a new shard if the index is missing" do
      values = Int64Values{
        Value[0_i64, 100, 1],
        Value[1_i64, 101, 1],
        Value[2_i64, 102, 1],
      }
      encoded = Encoding.encode(values)

      ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
        writer.write_block(1_i64, BlockStat.new(values), encoded)
      end

      FileUtils.rm("/tmp/appmonit-db/1/0-60.idx")

      (File::Stat.new("/tmp/appmonit-db/1/0-60.adb").size > 14).should be_true
      ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") { }
      File::Stat.new("/tmp/appmonit-db/1/0-60.adb").size.should eq 14
    end
  end
end
