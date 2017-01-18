module Appmonit::DB
  struct ADBFile
    getter root : String
    getter collection_name : String
    getter start_time : Time
    getter end_time : Time
    getter level : Int32
    getter sequence : Int32

    # expects file name to be like /collection-name/000000-1111/0000-0000.adb
    # first part is level second part is sequence
    def initialize(@root, @collection_name, @start_time, @end_time, @level, @sequence)
    end

    def initialize(location : String)
      if (match = ADB_REGEX.match(location))
        @root = match["root"]
        @collection_name = match["collection"]
        @start_time = Time.epoch(match["start_time"].to_i)
        @end_time = Time.epoch(match["end_time"].to_i)
        @level = match["level"].to_i
        @sequence = match["sequence"].to_i
      else
        raise InvalidFileName.new(location)
      end
    end

    def advance(level, sequence)
      ADBFile.new(@root, @collection_name, @start_time, @end_time, level, sequence)
    end

    def location
      File.join(@root, @collection_name, "#{@start_time.epoch}-#{@end_time.epoch}", "#{@level}-#{@sequence}.adb")
    end

    def index_location
      location.gsub(".adb", ".idx")
    end

    def <=>(other)
      if level == other.level
        sequence <=> other.sequence
      else
        level <=> other.level
      end
    end
  end
end
