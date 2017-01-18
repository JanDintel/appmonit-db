require "../spec_helper"

module Appmonit::DB
  describe ADBWriter do
    it "raises an exception when the file already exists" do
      ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") { }
      expect_raises DBExists do
        ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") { }
      end
    end

    it "writes the header when opened" do
      ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
      end
      file = File.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb")
      file.read(name = Bytes.new(FILE_HEADER.size))
      version = file.read_bytes(Int32)

      name.should eq FILE_HEADER
      version.should eq FILE_VERSION
    end

    it "writes the index when closed" do
      values = Int64Values{
        Value[Time.epoch(0), 100, 1],
        Value[Time.epoch(1), 101, 1],
        Value[Time.epoch(2), 102, 1],
      }

      collection_index = nil
      ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
        writer.write("my-column", values)
        collection_index = writer.collection_index
      end

      index = File.open("/tmp/appmonit-db/my-collection/0-60/1-0.idx")
      index.read(name = Bytes.new(FILE_HEADER.size))
      version = index.read_bytes(Int32)

      name.should eq FILE_HEADER
      version.should eq FILE_VERSION
      CollectionIndex.from_io(index).should eq collection_index
    end

    context "write values" do
      it "write the checksum and the block" do
        values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }

        ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
          writer.write("my-column", values)
        end

        encoded = Encoding.encode(values)

        file = File.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb")
        file.seek(14, IO::Seek::Set)

        checksum = file.read_bytes(Int64)
        buffer = Slice(UInt8).new(encoded.size)
        file.read_fully(buffer)
        Zlib.crc32(encoded).should eq checksum
        buffer.should eq encoded
      end
    end

    context "write block" do
      it "writes a block to the file" do
        values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }
        encoded = Encoding.encode(values)

        ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
          writer.write_block("my-column", BlockStat.new(values), encoded)
        end

        file = File.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb")
        file.seek(14, IO::Seek::Set)

        checksum = file.read_bytes(Int64)
        buffer = Slice(UInt8).new(encoded.size)
        file.read_fully(buffer)
        Zlib.crc32(encoded).should eq checksum
        buffer.should eq encoded
      end

      it "raises when the file is not locked" do
        values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }
        encoded = Encoding.encode(values)

        writer = ADBWriter.new("/tmp/appmonit-db/my-collection/0-60/1-0.adb")
        expect_raises MissingLockException do
          writer.write_block("my-column", BlockStat.new(values), encoded)
        end
      end
    end
  end
end
