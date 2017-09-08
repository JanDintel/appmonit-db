module Appmonit::DB
  module Util
    extend self

    def read_header(io)
      io.seek(0, IO::Seek::Set)
      io.read_fully(header = Bytes.new(10))
      version = io.read_bytes(Int32)
      if header != FILE_HEADER
        raise InvalidHeader.new
      elsif version != FILE_VERSION
        raise InvalidVersion.new("Expected #{FILE_VERSION} got #{version}")
      end
    end

    def write_header(io)
      io.write(FILE_HEADER)
      io.write_bytes(FILE_VERSION)
      io.flush
      FILE_HEADER.size + sizeof(Int32)
    end
  end
end
