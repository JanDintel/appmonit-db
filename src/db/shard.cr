module Appmonit::DB
  class Shard
    getter collection_name : String
    getter start_time : Time
    getter end_time : Time

    def self.open(location : String)
      shard = Shard.new(location)
      begin
        yield shard
      ensure
        shard.close
      end
    end

    def initialize(@location : String, @start_time : Time, @end_time : Time)
      if (match = COLLECTION_REGEX.match(@location))
        @collection_name = match["collection"]
      else
        raise InvalidFileName.new(@location)
      end
    end

    def initialize(@location : String)
      if (match = SHARD_REGEX.match(@location))
        @collection_name = match["collection"]
        @start_time = Time.epoch(match["start_time"].to_i)
        @end_time = Time.epoch(match["end_time"].to_i)
      else
        raise InvalidFileName.new(@location)
      end
    end

    def close
    end

    def files_for_level(level)
      file_level = level - 1
      files = relevant_files.select do |adb_file|
        if level < 4
          adb_file.level == file_level
        else
          adb_file.level >= file_level
        end
      end

      case level
      when 2
        files.size >= 2 ? files : [] of ADBFile
      when 3
        files.size >= 3 ? files : [] of ADBFile
      else # >= 4
        files.size >= 4 ? files : [] of ADBFile
      end
    end

    def last_adb_sequence
      if sorted_adb_files.size > 0
        sorted_adb_files.first.sequence # they are in reverse order
      else
        0
      end
    end

    def relevant_files
      minimum_sequence = 0

      groups_adb_files = sorted_adb_files.group_by(&.level)
      groups_adb_files.flat_map do |_, adb_files|
        current_minimum = minimum_sequence
        adb_files.select do |adb_file|
          minimum_sequence = {minimum_sequence, adb_file.sequence}.max
          adb_file.sequence >= current_minimum
        end
      end
    end

    # Sorting in reverse
    def sorted_adb_files
      adb_files.sort do |left, right|
        right <=> left
      end
    end

    private def adb_files
      Dir.glob(File.join(@location, "/*.adb")).map do |file_name|
        ADBFile.new(file_name)
      end
    end
  end
end
