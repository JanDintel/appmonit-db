require "../spec_helper"

module Appmonit::DB
  describe Collection do
    context "write" do
      it "can write values to the wal file" do
        values = Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(0), 100, 1.1],
          Value[Time.epoch(0), 100, true],
          Value[Time.epoch(0), 100, "a"],
          Value[Time.epoch(0), 100, ["a"]],
        }

        Collection.open("/tmp/appmonit-db/my-collection") do |collection|
          collection.write("my-column", values)
        end
      end
    end

    context "last wal sequence" do
      it "returns the last wal sequence for all files" do
        location = "/tmp/appmonit-db/my-collection"
        Dir.mkdir_p(location)
        File.write(File.join(location, "#{1.to_s.rjust(32, '0')}.wal"), "")
        File.write(File.join(location, "#{2.to_s.rjust(32, '0')}.wal"), "")
        File.write(File.join(location, "#{3.to_s.rjust(32, '0')}.wal"), "")

        Collection.new(location).last_wal_sequence.should eq 3
      end

      it "returns 0 if there are no WAL files yet" do
        location = "/tmp/appmonit-db/my-collection"
        Dir.mkdir_p(location)
        Collection.new(location).last_wal_sequence.should eq 0
      end
    end
  end
end
