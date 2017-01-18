require "../spec_helper"

module Appmonit::DB
  describe WALWriter do
    it "writes the header when opened" do
      WALWriter.open("/tmp/appmonit-db/my-collection") { }

      file = File.open("/tmp/appmonit-db/my-collection/#{1.to_s.rjust(32, '0')}.wal")
      file.read(name = Bytes.new(FILE_HEADER.size))
      version = file.read_bytes(Int32)

      name.should eq FILE_HEADER
      version.should eq FILE_VERSION
    end

    it "writes the values to the wal file" do
      values = Values{
        Value[Time.epoch(0), 100, 1],
        Value[Time.epoch(0), 100, 1.1],
        Value[Time.epoch(0), 100, true],
        Value[Time.epoch(0), 100, "a"],
        Value[Time.epoch(0), 100, ["a"]],
      }

      WALWriter.open("/tmp/appmonit-db/my-collection") do |writer|
        writer.write_values("my-column", values)
      end

      WALReader.open("/tmp/appmonit-db/my-collection") do |reader|
        read_values = reader.read_values("my-column", Time.epoch(0), Time.epoch(1))
        read_values.should eq values

        read_values = reader.read_values("my-column", Time.epoch(1), Time.epoch(2))
        read_values.should eq Values.new
      end
    end

    it "rolls the wal file if the size is exceeded" do
      values = Values{
        Value[Time.epoch(0), 100, 1],
        Value[Time.epoch(0), 100, 1.1],
        Value[Time.epoch(0), 100, true],
        Value[Time.epoch(0), 100, "a"],
        Value[Time.epoch(0), 100, ["a"]],
      }
      WALWriter.open("/tmp/appmonit-db/my-collection") do |writer|
        writer.max_wal_size = 14 + 71 # Header size + snappy encoded values size
        writer.write_values("my-column", values)
        writer.write_values("my-column", values)
      end

      WALReader.open("/tmp/appmonit-db/my-collection") do |reader|
        read_values = reader.read_values("my-column", Time.epoch(0), Time.epoch(1))
        read_values.should eq values + values
      end
    end
  end
end
