module Appmonit::DB
  struct ADBFile
    getter root : String
    getter collection_id : Int64
    getter min_epoch : Int64
    getter max_epoch : Int64

    # expects file name to be like /collection-id/min-max.adb
    # first part is level second part is sequence
    def initialize(@root, @collection_id, @min_epoch, @max_epoch)
    end

    def initialize(location : String)
      if (match = ADB_REGEX.match(location))
        @root = match["root"]
        @collection_id = match["collection"].to_i64
        @min_epoch = match["min_epoch"].to_i64
        @max_epoch = match["max_epoch"].to_i64
      else
        raise InvalidFileName.new(location)
      end
    end

    def location
      File.join(@root, @collection_id.to_s, "#{@min_epoch}-#{@max_epoch}.adb")
    end

    def index_location
      location.gsub(".adb", ".idx")
    end

    def tmp_index_location
      location.gsub(".adb", ".tidx")
    end
  end
end
