module Appmonit::DB
  describe BlockStat do
    context "update" do
      it "return the blockstat with updated offset" do
        values = Int64Values{Value[1_i64, 100, 0]}
        stat = BlockStat::Int64Values.new(values)
        stat.offset.should eq 0

        stat.update(offset: 20)
        stat.offset.should eq 20
      end
    end

    context "in range" do
      it "return true if the start and end time are in range" do
        values = Int64Values{Value[2_i64, 100, 0], Value[4_i64, 100, 0]}
        stat = BlockStat::Int64Values.new(values)
        stat.in_range(0_i64, 2_i64).should eq true
        stat.in_range(0_i64, 3_i64).should eq true
        stat.in_range(0_i64, 4_i64).should eq true
        stat.in_range(0_i64, 5_i64).should eq true
        stat.in_range(2_i64, 4_i64).should eq true
        stat.in_range(2_i64, 5_i64).should eq true
        stat.in_range(3_i64, 4_i64).should eq true
        stat.in_range(3_i64, 5_i64).should eq true
        stat.in_range(4_i64, 5_i64).should eq true
      end

      it "return false if the start and end time are in range" do
        values = Int64Values{Value[2_i64, 100, 0], Value[4_i64, 100, 0]}
        stat = BlockStat::Int64Values.new(values)
        stat.in_range(0_i64, 1_i64).should eq false
        stat.in_range(5_i64, 6_i64).should eq false
      end
    end

    context "from int64 values" do
      it "sets the size, min and max time" do
        values = Int64Values{
          Value[1_i64, 100, 0],
          Value[0_i64, 101, 1],
          Value[3_i64, 102, 2],
          Value[2_i64, 103, 3],
        }
        stat = BlockStat::Int64Values.new(values)
        stat.min_epoch.should eq 0_i64
        stat.max_epoch.should eq 3_i64
        stat.size.should eq 4
        values.size.should eq 4
      end

      it "sets the min, max and sum" do
        values = Int64Values{
          Value[1_i64, 100, 0],
          Value[0_i64, 101, 1],
          Value[3_i64, 102, 2],
          Value[2_i64, 103, 3],
        }
        stat = BlockStat::Int64Values.new(values)
        stat.min_value.should eq 0
        stat.max_value.should eq 3
        stat.sum_value.should eq 6
      end
    end

    context "from float64 values" do
      it "sets the size, min and max time" do
        values = Float64Values{
          Value[1_i64, 100, 0.0],
          Value[0_i64, 101, 1.0],
          Value[3_i64, 102, 2.0],
          Value[2_i64, 103, 3.0],
        }
        stat = BlockStat::Float64Values.new(values)
        stat.min_epoch.should eq 0_i64
        stat.max_epoch.should eq 3_i64
        stat.size.should eq 4
        values.size.should eq 4
      end

      it "sets the min, max and sum" do
        values = Float64Values{
          Value[1_i64, 100, 0.0],
          Value[0_i64, 101, 1.0],
          Value[3_i64, 102, 2.0],
          Value[2_i64, 103, 3.0],
        }
        stat = BlockStat::Float64Values.new(values)
        stat.min_value.should eq 0
        stat.max_value.should eq 3
        stat.sum_value.should eq 6
      end
    end

    context "from string values" do
      it "sets the size, min and max time" do
        values = StringValues{
          Value[1_i64, 100, "a"],
          Value[0_i64, 101, "b"],
          Value[3_i64, 102, "aa"],
          Value[2_i64, 103, "1"],
        }
        stat = BlockStat::StringValues.new(values)
        stat.min_epoch.should eq 0_i64
        stat.max_epoch.should eq 3_i64
        stat.size.should eq 4
        values.size.should eq 4
      end
    end

    context "from bool values" do
      it "sets the size, min and max time" do
        values = BoolValues{
          Value[1_i64, 100, true],
          Value[0_i64, 101, false],
        }
        stat = BlockStat::BoolValues.new(values)
        stat.min_epoch.should eq 0_i64
        stat.max_epoch.should eq 1_i64
        stat.size.should eq 2
        values.size.should eq 2
      end
    end

    context "from array values" do
      it "sets the size, min and max time" do
        values = ArrayValues{
          Value[1_i64, 100, ["a"]],
          Value[0_i64, 101, ["b"]],
          Value[3_i64, 102, ["aa"]],
          Value[2_i64, 103, ["1"]],
        }
        stat = BlockStat::ArrayValues.new(values)
        stat.min_epoch.should eq 0_i64
        stat.max_epoch.should eq 3_i64
        stat.size.should eq 4
        values.size.should eq 4
      end
    end

    context "encode and decode" do
      it "writes and reads BlockStat::Int64Values" do
        io = IO::Memory.new
        values = Int64Values{
          Value[1_i64, 100, 0],
          Value[0_i64, 101, 1],
        }
        BlockStat::Int64Values.new(values).to_io(io)
        io.rewind
        block_stat = BlockStat.from_io(io)
        block_stat.should be_a(BlockStat::Int64Values)
      end

      it "writes and reads BlockStat::Float64Values" do
        io = IO::Memory.new
        values = Float64Values{
          Value[1_i64, 100, 0.0],
          Value[0_i64, 101, 1.0],
        }
        BlockStat::Float64Values.new(values).to_io(io)
        io.rewind
        block_stat = BlockStat.from_io(io)
        block_stat.should be_a(BlockStat::Float64Values)
      end

      it "writes and reads BlockStat::BoolValues" do
        io = IO::Memory.new
        values = BoolValues{
          Value[1_i64, 100, true],
          Value[0_i64, 101, false],
        }
        BlockStat::BoolValues.new(values).to_io(io)
        io.rewind
        block_stat = BlockStat.from_io(io)
        block_stat.should be_a(BlockStat::BoolValues)
      end

      it "writes and reads BlockStat::StringValues" do
        io = IO::Memory.new
        values = StringValues{
          Value[1_i64, 100, "1"],
          Value[0_i64, 101, "2"],
        }
        BlockStat::StringValues.new(values).to_io(io)
        io.rewind
        block_stat = BlockStat.from_io(io)
        block_stat.should be_a(BlockStat::StringValues)
      end

      it "writes and reads BlockStat::ArrayValues" do
        io = IO::Memory.new
        values = ArrayValues{
          Value[1_i64, 100, ["0"]],
          Value[0_i64, 101, ["1"]],
        }
        BlockStat::ArrayValues.new(values).to_io(io)
        io.rewind
        block_stat = BlockStat.from_io(io)
        block_stat.should be_a(BlockStat::ArrayValues)
      end
    end
  end
end
