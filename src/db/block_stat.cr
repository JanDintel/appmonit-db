module Appmonit::DB
  module BlockStat
    getter size : Int32
    getter min_time : Time
    getter max_time : Time
    property offset : Int64

    def self.from_io(io)
      type = EncodingType.from_value(io.read_bytes(Int32))
      offset = io.read_bytes(Int64)
      size = io.read_bytes(Int32)
      min_time = Time.epoch(io.read_bytes(Int64))
      max_time = Time.epoch(io.read_bytes(Int64))

      case type
      when EncodingType::Int64
        BlockStat::Int64Values.from_io(io, offset, size, min_time, max_time)
      when EncodingType::Float64
        BlockStat::Float64Values.from_io(io, offset, size, min_time, max_time)
      when EncodingType::String
        BlockStat::StringValues.from_io(io, offset, size, min_time, max_time)
      when EncodingType::Bool
        BlockStat::BoolValues.new(offset, size, min_time, max_time)
      when EncodingType::Array
        BlockStat::ArrayValues.new(offset, size, min_time, max_time)
      else
        raise "Invalid encoding type"
      end
    end

    def self.new(values : ValuesType)
      case values
      when DB::Int64Values
        BlockStat::Int64Values.new(values)
      when DB::Float64Values
        BlockStat::Float64Values.new(values)
      when DB::BoolValues
        BlockStat::BoolValues.new(values)
      when DB::ArrayValues
        BlockStat::ArrayValues.new(values)
      when DB::StringValues
        BlockStat::StringValues.new(values)
      else
        raise "Invalid values type #{values.class}"
      end
    end

    def range
      (self.min_time..self.max_time)
    end

    def in_range(start_time, end_time)
      {min_time, start_time}.max <= {max_time, end_time}.min
    end

    def update(offset)
      @offset = offset.to_i64
      self
    end

    def to_io(io)
      io.write_bytes(encoding_type.value)
      io.write_bytes(offset)
      io.write_bytes(size)
      io.write_bytes(min_time.epoch)
      io.write_bytes(max_time.epoch)
    end
  end

  struct BlockStat::BoolValues
    include BlockStat

    def initialize(offset, @size, @min_time, @max_time)
      @offset = offset.to_i64
    end

    def initialize(values : DB::BoolValues)
      @offset = 0_i64
      @size = values.size
      first_value = values[0]
      @min_time = first_value.created_at
      @max_time = first_value.created_at

      # iterate over the values only once and skip the first
      # Don't use shift because it copies the array.
      1.upto(values.size - 1) do |index|
        value = values[index]
        @min_time = {@min_time, value.created_at}.min
        @max_time = {@max_time, value.created_at}.max
      end
    end

    def encoding_type
      EncodingType::Bool
    end
  end

  struct BlockStat::ArrayValues
    include BlockStat

    def initialize(offset, @size, @min_time, @max_time)
      @offset = offset.to_i64
    end

    def initialize(values : DB::ArrayValues)
      @offset = 0_i64
      @size = values.size
      first_value = values[0]
      @min_time = first_value.created_at
      @max_time = first_value.created_at

      # iterate over the values only once
      1.upto(values.size - 1) do |index|
        value = values[index]
        @min_time = {@min_time, value.created_at}.min
        @max_time = {@max_time, value.created_at}.max
      end
    end

    def encoding_type
      EncodingType::Array
    end
  end

  struct BlockStat::Int64Values
    include BlockStat

    getter min_value : Int64
    getter max_value : Int64
    getter sum_value : Int64

    def self.from_io(io, offset, size, min_time, max_time)
      min_value = io.read_bytes(Int64)
      max_value = io.read_bytes(Int64)
      sum_value = io.read_bytes(Int64)
      self.new(offset, size, min_time, max_time, min_value, max_value, sum_value)
    end

    def initialize(offset, @size, @min_time, @max_time, @min_value, @max_value, @sum_value)
      @offset = offset.to_i64
    end

    def initialize(values : DB::Int64Values)
      @offset = 0_i64
      @size = values.size
      @sum_value = 0_i64

      first_value = values[0]

      @min_time = first_value.created_at
      @max_time = first_value.created_at
      @min_value = first_value.value
      @max_value = first_value.value

      # iterate over the values only once
      1.upto(values.size - 1) do |index|
        value = values[index]
        @sum_value += value.value
        @min_time = {@min_time, value.created_at}.min
        @max_time = {@max_time, value.created_at}.max
        @min_value = {@min_value, value.value}.min
        @max_value = {@max_value, value.value}.max
      end
    end

    def to_io(io)
      super(io)
      io.write_bytes(min_value)
      io.write_bytes(max_value)
      io.write_bytes(sum_value)
    end

    def encoding_type
      EncodingType::Int64
    end
  end

  struct BlockStat::Float64Values
    include BlockStat

    getter min_value : Float64
    getter max_value : Float64
    getter sum_value : Float64

    def self.from_io(io, offset, size, min_time, max_time)
      min_value = io.read_bytes(Float64)
      max_value = io.read_bytes(Float64)
      sum_value = io.read_bytes(Float64)
      self.new(offset, size, min_time, max_time, min_value, max_value, sum_value)
    end

    def initialize(offset, @size, @min_time, @max_time, @min_value, @max_value, @sum_value)
      @offset = offset.to_i64
    end

    def initialize(values : DB::Float64Values)
      @offset = 0_i64
      @size = values.size
      @sum_value = 0.0_f64

      first_value = values[0]

      @min_time = first_value.created_at
      @max_time = first_value.created_at
      @min_value = first_value.value
      @max_value = first_value.value

      # iterate over the values only once
      1.upto(values.size - 1) do |index|
        value = values[index]
        @sum_value += value.value
        @min_time = {@min_time, value.created_at}.min
        @max_time = {@max_time, value.created_at}.max
        @min_value = {@min_value, value.value}.min
        @max_value = {@max_value, value.value}.max
      end
    end

    def to_io(io)
      super(io)
      io.write_bytes(min_value)
      io.write_bytes(max_value)
      io.write_bytes(sum_value)
    end

    def encoding_type
      EncodingType::Float64
    end
  end

  struct BlockStat::StringValues
    include BlockStat

    getter min_value : String
    getter max_value : String

    def self.from_io(io, offset, size, min_time, max_time)
      min_value = io.gets(io.read_bytes(Int32)).to_s
      max_value = io.gets(io.read_bytes(Int32)).to_s
      self.new(offset, size, min_time, max_time, min_value, max_value)
    end

    def initialize(offset, @size, @min_time, @max_time, @min_value, @max_value)
      @offset = offset.to_i64
    end

    def initialize(values : DB::StringValues)
      @offset = 0_i64
      @size = values.size

      first_value = values[0]

      @min_time = first_value.created_at
      @max_time = first_value.created_at
      @min_value = first_value.value
      @max_value = first_value.value

      # iterate over the values only once
      1.upto(values.size - 1) do |index|
        value = values[index]
        @min_time = {@min_time, value.created_at}.min
        @max_time = {@max_time, value.created_at}.max
        @min_value = {@min_value, value.value}.min
        @max_value = {@max_value, value.value}.max
      end
    end

    def to_io(io)
      super(io)
      io.write_bytes(min_value.size)
      io.write(min_value.to_slice)
      io.write_bytes(max_value.size)
      io.write(max_value.to_slice)
    end

    def encoding_type
      EncodingType::String
    end
  end
end
