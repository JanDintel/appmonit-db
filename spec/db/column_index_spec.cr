require "../spec_helper"

module Appmonit::DB
  describe ColumnIndex do
    it "contains the column name" do
      column_index = ColumnIndex.new("my-column")
      column_index.column_name.should eq "my-column"
      column_index.block_stats.should be_a(Array(BlockStat))
    end

    context "serialization" do
      it "can write to io" do
        io = IO::Memory.new
        block_stat = BlockStat::BoolValues.new(0, 0, 1, Time.epoch(0), Time.epoch(1))
        column_index = ColumnIndex.new("my-column")
        column_index.block_stats << block_stat
        column_index.to_io(io)
        io.rewind
        column_index = ColumnIndex.from_io(io)
        column_index.column_name.should eq "my-column"
        column_index.block_stats.first.should eq block_stat
      end
    end
  end
end
