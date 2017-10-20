require "../spec_helper"

module Appmonit::DB
  describe CollectionIndex do
    it "contains the column name" do
      collection_index = CollectionIndex.new(1_i64)
      collection_index.collection_id.should eq 1_i64
      collection_index.column_ids.should be_a(Hash(Int64, ColumnIndex))
    end

    context "serialization" do
      it "can write to io" do
        io = IO::Memory.new

        block_stat = BlockStat::BoolValues.new(0, 1, 0_i64, 1_i64)

        column_index = ColumnIndex.new(1_i64)
        column_index.block_stats << block_stat

        collection_index = CollectionIndex.new(1_i64)
        collection_index.column_ids[1_i64] = column_index
        collection_index.to_io(io)
        io.rewind

        collection_index = CollectionIndex.from_io(io)
        collection_index.collection_id.should eq 1_i64
        collection_index.column_ids[1_i64].should eq column_index
      end
    end
  end
end
