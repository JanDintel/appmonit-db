require "../spec_helper"

module Appmonit::DB
  describe Values do
    context "serialization" do
      it "can write and read from io" do
        values = Values{
          Value[0_i64, 100, 1],
          Value[0_i64, 100, 1.1],
          Value[0_i64, 100, true],
          Value[0_i64, 100, "a"],
          Value[0_i64, 100, ["a"]],
        }

        io = IO::Memory.new
        values.to_io(io)
        io.rewind
        Values.from_io(io).should eq values
      end
    end

    it "crops the values" do
      values = Int64Values{
        Value[0_i64, 100, 1],
        Value[1_i64, 100, 1],
        Value[2_i64, 100, 1],
        Value[3_i64, 100, 1],
        Value[4_i64, 100, 1],
      }

      values.crop!(1_i64, 3_i64)
      values.should eq Int64Values{
        Value[1_i64, 100, 1],
        Value[2_i64, 100, 1],
      }
    end

    context "{}" do
      it "accepts values from different types" do
        values = Values{
          Value[0_i64, 100, 1],
          Value[0_i64, 100, 1.1],
          Value[0_i64, 100, true],
          Value[0_i64, 100, "a"],
          Value[0_i64, 100, ["a"]],
        }

        values[0].should be_a(Int64Value)
        values[1].should be_a(Float64Value)
        values[2].should be_a(BoolValue)
        values[3].should be_a(StringValue)
        values[4].should be_a(ArrayValue)
      end
    end

    context "Int64Values" do
      it "contains only Int64" do
        values = Int64Values{Value[0_i64, 100, 1], Value[0_i64, 100, 1]}

        values.first.should be_a(Int64Value)
        values.last.should be_a(Int64Value)
      end

      it "raises when trying to set a non Int64Value" do
        expect_raises do
          Int64Values{Value[0_i64, 100, "a"]}
        end
      end
    end

    context "Float64Values" do
      it "contains only Float64" do
        values = Float64Values{Value[0_i64, 100, 1.1], Value[0_i64, 100, 1.1]}

        values.first.should be_a(Float64Value)
        values.last.should be_a(Float64Value)
      end

      it "raises when trying to set a non Float64Value" do
        expect_raises do
          Float64Values{Value[0_i64, 100, "a"]}
        end
      end
    end

    context "BoolValues" do
      it "contains only Bool" do
        values = BoolValues{Value[0_i64, 100, true], Value[0_i64, 100, false]}

        values.first.should be_a(BoolValue)
        values.last.should be_a(BoolValue)
      end

      it "raises when trying to set a non BoolValue" do
        expect_raises do
          BoolValues{Value[0_i64, 100, "a"]}
        end
      end
    end

    context "StringValues" do
      it "contains only String" do
        values = StringValues{Value[0_i64, 100, "a"], Value[0_i64, 100, "a"]}

        values.first.should be_a(StringValue)
        values.last.should be_a(StringValue)
      end

      it "raises when trying to set a non StringValue" do
        expect_raises do
          StringValues{Value[0_i64, 100, 1]}
        end
      end
    end

    context "ArrayValues" do
      it "contains only Array" do
        values = ArrayValues{Value[0_i64, 100, ["a"]], Value[0_i64, 100, ["a"]]}

        values.first.should be_a(ArrayValue)
        values.last.should be_a(ArrayValue)
      end

      it "raises when trying to set a non ArrayValue" do
        expect_raises do
          ArrayValues{Value[0_i64, 100, "a"]}
        end
      end
    end
  end
end
