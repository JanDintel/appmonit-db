module Appmonit::DB
  class Exception < ::Exception
  end

  class MissingLockException < Exception
  end

  class ChecksumFailed < Exception
  end

  class InvalidFileName < Exception
    def initialize(name)
      super("Filename is invalid: #{name}")
    end
  end

  class InvalidHeader < Exception
  end

  class InvalidVersion < Exception
  end

  class InvalidEncoding < Exception
  end

  class DBMissing < Exception
    def initialize(location)
      super("File missing: #{location}")
    end
  end

  class DBExists < Exception
    def initialize(location)
      super("File already exists: #{location}")
    end
  end
end
