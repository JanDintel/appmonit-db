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
end
