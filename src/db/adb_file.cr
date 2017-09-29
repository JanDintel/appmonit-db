module Appmonit::DB
  struct ADBFile
    getter root : String
    getter collection_id : Int64
    getter min_time : Time
    getter max_time : Time

    # expects file name to be like /collection-id/min-max.adb
    # first part is level second part is sequence
    def initialize(@root, @collection_id, @min_time, @max_time)
    end

    def initialize(location : String)
      if (match = ADB_REGEX.match(location))
        @root = match["root"]
        @collection_id = match["collection"].to_i64
        @min_time = Time.epoch(match["min_time"].to_i64)
        @max_time = Time.epoch(match["max_time"].to_i64)
      else
        raise InvalidFileName.new(location)
      end
    end

    def location
      File.join(@root, @collection_id.to_s, "#{@min_time.epoch}-#{@max_time.epoch}.adb")
    end

    def index_location
      location.gsub(".adb", ".idx")
    end

    def tmp_index_location
      location.gsub(".adb", ".tidx")
    end
  end
end
