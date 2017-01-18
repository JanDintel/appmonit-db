module Appmonit::DB
  record WALFile, sequence : Int32, file_name : String do
    def initialize(@file_name : String)
      @sequence = File.basename(@file_name, ".wal").to_i
    end

    def initialize(location, @sequence)
      @file_name = File.join(location, "#{@sequence.to_s.rjust(32, '0')}.cur")
    end

    def close
      FileUtils.rename(@file_name, @file_name.gsub(".cur", ".wal"))
    end
  end
end
