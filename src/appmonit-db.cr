require "file_utils"
require "zlib"

require "./db/*"

module Appmonit::DB
  FILE_HEADER      = "appmonitdb".to_slice
  FILE_VERSION     =    1
  VALUES_PER_BLOCK = 5000

  alias ValueType = Int64 | Float64 | String | Bool | Array(String)
  alias ValuesType = Int64Values | Float64Values | BoolValues | StringValues | ArrayValues

  COLLECTION_REGEX = /.*\/(?<collection>[^\\]+)\/.*/
  SHARD_REGEX      = /.*\/(?<collection>[^\\]+)\/(?<start_time>\d+)-(?<end_time>\d+)\/.*/
  ADB_REGEX        = /(?<root>.*)\/(?<collection>[^\\]+)\/(?<start_time>\d+)-(?<end_time>\d+)\/(?<level>\d+)-(?<sequence>\d+)\.adb/

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
