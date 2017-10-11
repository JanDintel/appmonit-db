require "../spec_helper"

module Appmonit::DB
  describe "Encoding" do
    context "Int64 Encoding" do
      it "encodes and decodes Int64Values" do
        values = Int64Values{Value[Time.epoch(0), 100, 1], Value[Time.epoch(1), 100, 1]}

        encoded = Encoding.encode(values)
        encoded.should be_a(Bytes)

        decoded = Encoding.decode(encoded, EncodingType::Int64)
        decoded.should eq values

        iterator = Encoding.iterate(encoded, EncodingType::Int64)
        iterator.next.should eq Value[Time.epoch(0), 100, 1]
        iterator.next.should eq Value[Time.epoch(1), 100, 1]
      end
    end

    context "Float64 Encoding" do
      it "encodes and decodes Float64Values" do
        values = Float64Values{Value[Time.epoch(0), 100, 1.1], Value[Time.epoch(1), 100, 2.1]}

        encoded = Encoding.encode(values)
        encoded.should be_a(Bytes)

        decoded = Encoding.decode(encoded, EncodingType::Float64)
        decoded.should eq values

        iterator = Encoding.iterate(encoded, EncodingType::Float64)
        iterator.next.should eq Value[Time.epoch(0), 100, 1.1]
        iterator.next.should eq Value[Time.epoch(1), 100, 2.1]
      end
    end

    context "String Encoding" do
      it "encodes and decodes StringValues" do
        values = StringValues{Value[Time.epoch(0), 100, "a"], Value[Time.epoch(1), 100, "b"]}

        encoded = Encoding.encode(values)
        encoded.should be_a(Bytes)

        decoded = Encoding.decode(encoded, EncodingType::String)
        decoded.should eq values

        iterator = Encoding.iterate(encoded, EncodingType::String)
        iterator.next.should eq Value[Time.epoch(0), 100, "a"]
        iterator.next.should eq Value[Time.epoch(1), 100, "b"]
      end
    end

    context "Array Encoding" do
      it "encodes and decodes ArrayValues" do
        values = ArrayValues{Value[Time.epoch(0), 100, ["a"]], Value[Time.epoch(1), 100, ["b"]]}

        encoded = Encoding.encode(values)
        encoded.should be_a(Bytes)

        decoded = Encoding.decode(encoded, EncodingType::Array)
        decoded.should eq values

        iterator = Encoding.iterate(encoded, EncodingType::Array)
        iterator.next.should eq Value[Time.epoch(0), 100, ["a"]]
        iterator.next.should eq Value[Time.epoch(1), 100, ["b"]]
      end
    end

    context "Bool Encoding" do
      it "encodes and decodes BoolValues" do
        values = BoolValues{Value[Time.epoch(0), 100, true], Value[Time.epoch(1), 100, false]}

        encoded = Encoding.encode(values)
        encoded.should be_a(Bytes)

        decoded = Encoding.decode(encoded, EncodingType::Bool)
        decoded.should eq values

        iterator = Encoding.iterate(encoded, EncodingType::Bool)
        iterator.next.should eq Value[Time.epoch(0), 100, true]
        iterator.next.should eq Value[Time.epoch(1), 100, false]
      end
    end
  end
end
