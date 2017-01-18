require "../spec_helper"

module Appmonit::DB
  describe Shard do
    it "initializes with the correct times" do
      location = "/tmp/appmonit-db/my-collection/0-60/"
      shard = Shard.new(location)
      shard.start_time.should eq Time.epoch(0)
      shard.end_time.should eq Time.epoch(60)
    end

    context "files_for_level" do
      it "returns the relevant adb files for a shard" do
        location = "/tmp/appmonit-db/my-collection/0-60/"
        Dir.mkdir_p(location)

        File.write(File.join(location, "1-00000001.adb"), "")
        File.write(File.join(location, "1-00000002.adb"), "")
        File.write(File.join(location, "2-00000003.adb"), "")
        File.write(File.join(location, "1-00000004.adb"), "")
        File.write(File.join(location, "1-00000005.adb"), "")

        Shard.new(location).files_for_level(2).first.sequence.should eq 5
        Shard.new(location).files_for_level(2).last.sequence.should eq 4
      end
    end

    context "last adb sequence" do
      it "returns the last sequence for all files" do
        location = "/tmp/appmonit-db/my-collection/0-60/"
        Dir.mkdir_p(location)
        File.write(File.join(location, "1-00000002.adb"), "")

        Shard.new(location).last_adb_sequence.should eq 2
      end

      it "returns the last sequence for all files" do
        location = "/tmp/appmonit-db/my-collection/0-60/"
        Dir.mkdir_p(location)
        File.write(File.join(location, "1-00000001.adb"), "")
        File.write(File.join(location, "1-00000002.adb"), "")

        Shard.new(location).last_adb_sequence.should eq 2
      end

      it "returns 0 if there are no ADB files yet" do
        location = "/tmp/appmonit-db/my-collection/0-60/"
        Dir.mkdir_p(location)
        Shard.new(location).last_adb_sequence.should eq 0
      end
    end

    context "relevant files" do
      it "returns the relevant files" do
        location = "/tmp/appmonit-db/my-collection/0-60/"
        Dir.mkdir_p(location)

        File.write(File.join(location, "19-00000240.adb"), "")
        File.write(File.join(location, "9-00000196.adb"), "")
        File.write(File.join(location, "3-00000226.adb"), "")
        File.write(File.join(location, "1-00000241.adb"), "")

        Shard.new(location).relevant_files.should eq [
          ADBFile.new(File.join(location, "19-00000240.adb")),
          ADBFile.new(File.join(location, "1-00000241.adb")),
        ]
      end
    end
  end
end
