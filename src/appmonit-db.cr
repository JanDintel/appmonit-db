require "file_utils"
require "crc32"

require "./db/*"

module Appmonit::DB
  FILE_HEADER      = "appmonitdb".to_slice
  FILE_VERSION     =    1
  VALUES_PER_BLOCK = 5000

  alias ValueType = Int64 | Float64 | String | Bool | Array(String)
  alias ValuesType = Array(Int64Value) | Array(Float64Value) | Array(BoolValue) | Array(StringValue) | Array(ArrayValue)

  ADB_REGEX = /(?<root>.*)\/(?<collection>\d+)\/(?<min_epoch>\d+)-(?<max_epoch>\d+)\.adb/

  @[Flags]
  enum EncodingType
    Int64
    Float64
    String
    Bool
    Array
    Numeric = Int64 | Float64
  end

  def self.convert_values(values : Array(Value)) : ValuesType
    case values.first
    when Int64Value
      converted = Array(Int64Value).new
      values.each do |value|
        converted << value if value.is_a?(Int64Value)
      end
      converted
    when Float64Value
      converted = Array(Float64Value).new
      values.each do |value|
        converted << value if value.is_a?(Float64Value)
      end
      converted
    when BoolValue
      converted = Array(BoolValue).new
      values.each do |value|
        converted << value if value.is_a?(BoolValue)
      end
      converted
    when StringValue
      converted = Array(StringValue).new
      values.each do |value|
        converted << value if value.is_a?(StringValue)
      end
      converted
    when ArrayValue
      converted = Array(ArrayValue).new
      values.each do |value|
        converted << value if value.is_a?(ArrayValue)
      end
      converted
    else
      raise "Invalid encoding"
    end
  end
end
