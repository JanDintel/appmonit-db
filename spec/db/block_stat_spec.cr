module Appmonit::DB
  describe BlockStat do
    context "update" do
      it "return the blockstat with updated offset and byte size" do
        values = Int64Values{Value[Time.epoch(1), 100, 0]}
        stat = BlockStat::Int64Values.new(values)
        stat.offset.should eq 0
        stat.byte_size.should eq 0

        stat.update(byte_size: 10, offset: 20)
        stat.offset.should eq 20
        stat.byte_size.should eq 10
      end
    end

    context "in range" do
      it "return true if the start and end time are in range" do
        values = Int64Values{Value[Time.epoch(2), 100, 0], Value[Time.epoch(4), 100, 0]}
        stat = BlockStat::Int64Values.new(values)
        stat.in_range(Time.epoch(0), Time.epoch(2)).should eq true
        stat.in_range(Time.epoch(0), Time.epoch(3)).should eq true
        stat.in_range(Time.epoch(0), Time.epoch(4)).should eq true
        stat.in_range(Time.epoch(0), Time.epoch(5)).should eq true
        stat.in_range(Time.epoch(2), Time.epoch(4)).should eq true
        stat.in_range(Time.epoch(2), Time.epoch(5)).should eq true
        stat.in_range(Time.epoch(3), Time.epoch(4)).should eq true
        stat.in_range(Time.epoch(3), Time.epoch(5)).should eq true
        stat.in_range(Time.epoch(4), Time.epoch(5)).should eq true
      end

      it "return false if the start and end time are in range" do
        values = Int64Values{Value[Time.epoch(2), 100, 0], Value[Time.epoch(4), 100, 0]}
        stat = BlockStat::Int64Values.new(values)
        stat.in_range(Time.epoch(0), Time.epoch(1)).should eq false
        stat.in_range(Time.epoch(5), Time.epoch(6)).should eq false
      end
    end

    context "from int64 values" do
      it "sets the size, min and max time" do
        values = Int64Values{
          Value[Time.epoch(1), 100, 0],
          Value[Time.epoch(0), 101, 1],
          Value[Time.epoch(3), 102, 2],
          Value[Time.epoch(2), 103, 3],
        }
        stat = BlockStat::Int64Values.new(values)
        stat.min_time.should eq Time.epoch(0)
        stat.max_time.should eq Time.epoch(3)
        stat.size.should eq 4
        values.size.should eq 4
      end

      it "sets the min, max and sum" do
        values = Int64Values{
          Value[Time.epoch(1), 100, 0],
          Value[Time.epoch(0), 101, 1],
          Value[Time.epoch(3), 102, 2],
          Value[Time.epoch(2), 103, 3],
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
          Value[Time.epoch(1), 100, 0.0],
          Value[Time.epoch(0), 101, 1.0],
          Value[Time.epoch(3), 102, 2.0],
          Value[Time.epoch(2), 103, 3.0],
        }
        stat = BlockStat::Float64Values.new(values)
        stat.min_time.should eq Time.epoch(0)
        stat.max_time.should eq Time.epoch(3)
        stat.size.should eq 4
        values.size.should eq 4
      end

      it "sets the min, max and sum" do
        values = Float64Values{
          Value[Time.epoch(1), 100, 0.0],
          Value[Time.epoch(0), 101, 1.0],
          Value[Time.epoch(3), 102, 2.0],
          Value[Time.epoch(2), 103, 3.0],
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
          Value[Time.epoch(1), 100, "a"],
          Value[Time.epoch(0), 101, "b"],
          Value[Time.epoch(3), 102, "aa"],
          Value[Time.epoch(2), 103, "1"],
        }
        stat = BlockStat::StringValues.new(values)
        stat.min_time.should eq Time.epoch(0)
        stat.max_time.should eq Time.epoch(3)
        stat.size.should eq 4
        values.size.should eq 4
      end

      it "sets the min, max and sum" do
        values = StringValues{
          Value[Time.epoch(1), 100, "a"],
          Value[Time.epoch(0), 101, "b"],
          Value[Time.epoch(3), 102, "aa"],
          Value[Time.epoch(2), 103, "1"],
        }
        stat = BlockStat::StringValues.new(values)
        stat.min_value.should eq "1"
        stat.max_value.should eq "b"
      end
    end

    context "from bool values" do
      it "sets the size, min and max time" do
        values = BoolValues{
          Value[Time.epoch(1), 100, true],
          Value[Time.epoch(0), 101, false],
        }
        stat = BlockStat::BoolValues.new(values)
        stat.min_time.should eq Time.epoch(0)
        stat.max_time.should eq Time.epoch(1)
        stat.size.should eq 2
        values.size.should eq 2
      end
    end

    context "from array values" do
      it "sets the size, min and max time" do
        values = ArrayValues{
          Value[Time.epoch(1), 100, ["a"]],
          Value[Time.epoch(0), 101, ["b"]],
          Value[Time.epoch(3), 102, ["aa"]],
          Value[Time.epoch(2), 103, ["1"]],
        }
        stat = BlockStat::ArrayValues.new(values)
        stat.min_time.should eq Time.epoch(0)
        stat.max_time.should eq Time.epoch(3)
        stat.size.should eq 4
        values.size.should eq 4
      end
    end

    context "encode and decode" do
      it "writes and reads BlockStat::Int64Values" do
        io = IO::Memory.new
        values = Int64Values{
          Value[Time.epoch(1), 100, 0],
          Value[Time.epoch(0), 101, 1],
        }
        BlockStat::Int64Values.new(values).to_io(io)
        io.rewind
        block_stat = BlockStat.from_io(io)
        block_stat.should be_a(BlockStat::Int64Values)
      end

      it "writes and reads BlockStat::Float64Values" do
        io = IO::Memory.new
        values = Float64Values{
          Value[Time.epoch(1), 100, 0.0],
          Value[Time.epoch(0), 101, 1.0],
        }
        BlockStat::Float64Values.new(values).to_io(io)
        io.rewind
        block_stat = BlockStat.from_io(io)
        block_stat.should be_a(BlockStat::Float64Values)
      end

      it "writes and reads BlockStat::BoolValues" do
        io = IO::Memory.new
        values = BoolValues{
          Value[Time.epoch(1), 100, true],
          Value[Time.epoch(0), 101, false],
        }
        BlockStat::BoolValues.new(values).to_io(io)
        io.rewind
        block_stat = BlockStat.from_io(io)
        block_stat.should be_a(BlockStat::BoolValues)
      end

      it "writes and reads BlockStat::StringValues" do
        io = IO::Memory.new
        values = StringValues{
          Value[Time.epoch(1), 100, "1"],
          Value[Time.epoch(0), 101, "2"],
        }
        BlockStat::StringValues.new(values).to_io(io)
        io.rewind
        block_stat = BlockStat.from_io(io)
        block_stat.should be_a(BlockStat::StringValues)
      end

      it "writes and reads BlockStat::ArrayValues" do
        io = IO::Memory.new
        values = ArrayValues{
          Value[Time.epoch(1), 100, ["0"]],
          Value[Time.epoch(0), 101, ["1"]],
        }
        BlockStat::ArrayValues.new(values).to_io(io)
        io.rewind
        block_stat = BlockStat.from_io(io)
        block_stat.should be_a(BlockStat::ArrayValues)
      end
    end
  end
end
