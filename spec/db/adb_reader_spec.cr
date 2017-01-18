require "../spec_helper"

module Appmonit::DB
  describe ADBReader do
    it "raises an exception when the file does not exist" do
      expect_raises DBMissing do
        ADBReader.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") { }
      end
    end

    it "raises an exception when the index does not exist" do
      ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") { }
      File.delete("/tmp/appmonit-db/my-collection/0-60/1-0.adb")
      expect_raises DBMissing do
        ADBReader.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") { }
      end
    end

    it "reads the index" do
      values = Int64Values{
        Value[Time.epoch(0), 100, 1],
        Value[Time.epoch(1), 101, 1],
        Value[Time.epoch(2), 102, 1],
      }

      collection_index = nil

      ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
        writer.write("my-column", values)
        collection_index = writer.collection_index
      end

      ADBReader.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |reader|
        reader.collection_index.should eq collection_index
      end
    end

    context "column_names" do
      it "returns the columns names" do
        values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }
        encoded = Encoding.encode(values)

        ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
          writer.write_block("other-column", BlockStat.new(values), encoded)
          writer.write_block("my-column", BlockStat.new(values), encoded)
        end

        ADBReader.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |reader|
          reader.column_names.should eq ["my-column", "other-column"]
        end
      end
    end

    context "encoding_types" do
      it "returns the encoding_types for a column" do
        int64values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }
        int64encoded = Encoding.encode(int64values)

        float64values = Float64Values{
          Value[Time.epoch(0), 100, 1.0],
          Value[Time.epoch(1), 101, 1.1],
          Value[Time.epoch(2), 102, 1.2],
        }
        float64encoded = Encoding.encode(float64values)

        string_values = StringValues{
          Value[Time.epoch(0), 100, "a"],
          Value[Time.epoch(1), 101, "b"],
          Value[Time.epoch(2), 102, "c"],
        }
        string_encoded = Encoding.encode(string_values)

        ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
          writer.write_block("my-column", BlockStat.new(int64values), int64encoded)
          writer.write_block("my-column", BlockStat.new(float64values), float64encoded)
          writer.write_block("my-column", BlockStat.new(string_values), string_encoded)
        end

        ADBReader.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |reader|
          reader.encoding_types("my-column").should eq [EncodingType::Int64, EncodingType::Float64, EncodingType::String]
        end
      end
    end

    context "read_block" do
      it "reads a block" do
        values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }
        encoded = Encoding.encode(values)

        ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
          writer.write_block("my-column", BlockStat.new(values), encoded)
        end

        ADBReader.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |reader|
          block_stat = reader.collection_index.columns["my-column"].block_stats.first
          encoded = reader.read_block(block_stat.offset, block_stat.byte_size)

          Encoding.decode(encoded, block_stat.encoding_type).should eq values
        end
      end
    end

    context "read_values" do
      it "reads all value types for a column" do
        int64values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }
        int64encoded = Encoding.encode(int64values)

        float64values = Float64Values{
          Value[Time.epoch(0), 100, 1.0],
          Value[Time.epoch(1), 101, 1.1],
          Value[Time.epoch(2), 102, 1.2],
        }
        float64encoded = Encoding.encode(float64values)

        string_values = StringValues{
          Value[Time.epoch(0), 100, "a"],
          Value[Time.epoch(1), 101, "b"],
          Value[Time.epoch(2), 102, "c"],
        }
        string_encoded = Encoding.encode(string_values)

        ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
          writer.write_block("my-column", BlockStat.new(int64values), int64encoded)
          writer.write_block("my-column", BlockStat.new(float64values), float64encoded)
          writer.write_block("my-column", BlockStat.new(string_values), string_encoded)
        end

        ADBReader.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |reader|
          values = reader.read_values("my-column")
          values.should be_a(Values)
          values.should eq int64values + float64values + string_values
        end
      end

      it "reads the values for a time range" do
        int64values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }
        int64encoded = Encoding.encode(int64values)

        ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
          writer.write_block("my-column", BlockStat.new(int64values), int64encoded)
        end

        ADBReader.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |reader|
          values = reader.read_values("my-column", Time.epoch(2), Time.epoch(3))
          values.should be_a(Values)
          values.should eq Int64Values{
            Value[Time.epoch(2), 102, 1],
          }
        end
      end

      it "reads value types for a column and type" do
        int64values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }
        int64encoded = Encoding.encode(int64values)

        float64values = Float64Values{
          Value[Time.epoch(0), 100, 1.0],
          Value[Time.epoch(1), 101, 1.1],
          Value[Time.epoch(2), 102, 1.2],
        }
        float64encoded = Encoding.encode(float64values)

        string_values = StringValues{
          Value[Time.epoch(0), 100, "a"],
          Value[Time.epoch(1), 101, "b"],
          Value[Time.epoch(2), 102, "c"],
        }
        string_encoded = Encoding.encode(string_values)

        ADBWriter.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |writer|
          writer.write_block("my-column", BlockStat.new(int64values), int64encoded)
          writer.write_block("my-column", BlockStat.new(float64values), float64encoded)
          writer.write_block("my-column", BlockStat.new(string_values), string_encoded)
        end

        ADBReader.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |reader|
          values = reader.read_values("my-column", EncodingType::Numeric)
          values.should be_a(Values)
          values.should eq int64values + float64values
        end

        ADBReader.open("/tmp/appmonit-db/my-collection/0-60/1-0.adb") do |reader|
          values = reader.read_values("my-column", EncodingType::Int64)
          values.should be_a(Values)
          values.should eq int64values
        end
      end
    end
  end
end
