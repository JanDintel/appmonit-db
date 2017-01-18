require "../spec_helper"

module Appmonit::DB
  describe CollectionIndex do
    it "contains the column name" do
      collection_index = CollectionIndex.new("my-collection")
      collection_index.collection_name.should eq "my-collection"
      collection_index.columns.should be_a(Hash(String, ColumnIndex))
    end

    context "serialization" do
      it "can write to io" do
        io = IO::Memory.new

        block_stat = BlockStat::BoolValues.new(0, 0, 1, Time.epoch(0), Time.epoch(1))

        column_index = ColumnIndex.new("my-column")
        column_index.block_stats << block_stat

        collection_index = CollectionIndex.new("my-collection")
        collection_index.columns["my-column"] = column_index
        collection_index.to_io(io)
        io.rewind

        collection_index = CollectionIndex.from_io(io)
        collection_index.collection_name.should eq "my-collection"
        collection_index.columns["my-column"].should eq column_index
      end
    end
  end
end
