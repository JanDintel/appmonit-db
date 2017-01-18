require "../spec_helper"

module Appmonit::DB
  describe Value do
    context "serialization" do
      it "can write and read to io" do
        value = Value[Time.epoch(0), 100, 1]
        io = IO::Memory.new
        value.to_io(io)
        io.rewind
        Value.from_io(io).should eq value

        value = Value[Time.epoch(0), 100, 1.1]
        io = IO::Memory.new
        value.to_io(io)
        io.rewind
        Value.from_io(io).should eq value

        value = Value[Time.epoch(0), 100, true]
        io = IO::Memory.new
        value.to_io(io)
        io.rewind
        Value.from_io(io).should eq value

        value = Value[Time.epoch(0), 100, "string"]
        io = IO::Memory.new
        value.to_io(io)
        io.rewind
        Value.from_io(io).should eq value

        value = Value[Time.epoch(0), 100, ["array"]]
        io = IO::Memory.new
        value.to_io(io)
        io.rewind
        Value.from_io(io).should eq value
      end
    end

    context "Value[]" do
      it "creates an Int64 value" do
        value = Value[Time.epoch(0), 100, 1]
        value.should be_a(Int64Value)
        value.created_at.should eq Time.epoch(0)
        value.uuid.should eq 100
        value.value.should be_a(Int64)
        value.value.should eq 1
      end

      it "creates an Float64 value" do
        value = Value[Time.epoch(0), 100, 1.1]
        value.should be_a(Float64Value)
        value.created_at.should eq Time.epoch(0)
        value.uuid.should eq 100
        value.value.should be_a(Float64)
        value.value.should eq 1.1
      end

      it "creates an Bool value" do
        value = Value[Time.epoch(0), 100, true]
        value.should be_a(BoolValue)
        value.created_at.should eq Time.epoch(0)
        value.uuid.should eq 100
        value.value.should be_a(Bool)
        value.value.should eq true
      end

      it "creates an String value" do
        value = Value[Time.epoch(0), 100, "string"]
        value.should be_a(StringValue)
        value.created_at.should eq Time.epoch(0)
        value.uuid.should eq 100
        value.value.should be_a(String)
        value.value.should eq "string"
      end

      it "creates an Array value" do
        value = Value[Time.epoch(0), 100, [1, "string", true, 1.1]]
        value.should be_a(ArrayValue)
        value.created_at.should eq Time.epoch(0)
        value.uuid.should eq 100
        value.value.should be_a(Array(String))
        value.value.should eq ["1", "string", "true", "1.1"]
      end
    end
  end
end
