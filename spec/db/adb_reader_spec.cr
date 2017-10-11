require "../spec_helper"

module Appmonit::DB
  describe ADBReader do
    it "raises an exception when the file does not exist" do
      expect_raises DBMissing do
        ADBReader.open("/tmp/appmonit-db/1/0-60.adb") { }
      end
    end

    it "raises an exception when the index does not exist" do
      ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") { }
      File.delete("/tmp/appmonit-db/1/0-60.adb")
      expect_raises DBMissing do
        ADBReader.open("/tmp/appmonit-db/1/0-60.adb") { }
      end
    end

    it "reads the index" do
      values = Int64Values{
        Value[Time.epoch(0), 100, 1],
        Value[Time.epoch(1), 101, 1],
        Value[Time.epoch(2), 102, 1],
      }

      collection_index = nil

      ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
        writer.write(1_i64, values)
        collection_index = writer.collection_index
      end

      ADBReader.open("/tmp/appmonit-db/1/0-60.adb") do |reader|
        reader.collection_index.should eq collection_index
      end
    end

    context "column_ids" do
      it "returns the columns ids" do
        values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
        }
        encoded = Encoding.encode(values)

        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.write_block(1_i64, BlockStat.new(values), encoded)
          writer.write_block(2_i64, BlockStat.new(values), encoded)
        end

        ADBReader.open("/tmp/appmonit-db/1/0-60.adb") do |reader|
          reader.column_ids.should eq [1_i64, 2_i64]
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

        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.write_block(1_i64, BlockStat.new(values), encoded)
        end

        ADBReader.open("/tmp/appmonit-db/1/0-60.adb") do |reader|
          block_stat = reader.collection_index.column_ids[1_i64].block_stats.first
          encoded = reader.read_block(block_stat.offset)

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

        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.write_block(1_i64, BlockStat.new(int64values), int64encoded)
          writer.write_block(1_i64, BlockStat.new(float64values), float64encoded)
          writer.write_block(1_i64, BlockStat.new(string_values), string_encoded)
        end

        ADBReader.open("/tmp/appmonit-db/1/0-60.adb") do |reader|
          values = reader.read_values(1_i64)
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

        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.write_block(1_i64, BlockStat.new(int64values), int64encoded)
        end

        ADBReader.open("/tmp/appmonit-db/1/0-60.adb") do |reader|
          values = reader.read_values(1_i64, Time.epoch(2), Time.epoch(3))
          values.should be_a(Values)
          values.should eq Int64Values{
            Value[Time.epoch(2), 102, 1],
          }
        end
      end

      it "iterates the values for a time range in order" do
        int64values = Int64Values{
          Value[Time.epoch(0), 100, 1],
          Value[Time.epoch(1), 101, 1],
          Value[Time.epoch(2), 102, 1],
          Value[Time.epoch(4), 103, 1],
        }
        int64encoded = Encoding.encode(int64values)

        float64values = Float64Values{
          Value[Time.epoch(2), 100, 1.1],
          Value[Time.epoch(3), 100, 1.1],
        }
        float64encoded = Encoding.encode(float64values)

        ADBWriter.open("/tmp/appmonit-db/1/0-60.adb") do |writer|
          writer.write_block(1_i64, BlockStat.new(int64values), int64encoded)
          writer.write_block(1_i64, BlockStat.new(float64values), float64encoded)
          writer.write_block(1_i64, BlockStat.new(int64values), int64encoded)
        end

        ADBReader.open("/tmp/appmonit-db/1/0-60.adb") do |reader|
          iterator = reader.iterate(1_i64, Time.epoch(1), Time.epoch(3))
          values = Array(Value).new
          iterator.each do |value|
            values << value if value
          end
          values.should eq [
            Value[Time.epoch(1), 101, 1],
            Value[Time.epoch(1), 101, 1],
            Value[Time.epoch(2), 100, 1.1],
            Value[Time.epoch(2), 102, 1],
            Value[Time.epoch(2), 102, 1],
            Value[Time.epoch(3), 100, 1.1],
          ]
        end
      end
    end
  end
end
