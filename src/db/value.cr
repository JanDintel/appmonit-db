module Appmonit::DB
  module Value
    abstract def created_at
    abstract def uuid
    abstract def value
    abstract def encoding_type

    def self.from_io(io)
      encoding_type = EncodingType.from_value(io.read_bytes(Int32))
      case encoding_type
      when EncodingType::Int64
        Int64Value.from_io(io)
      when EncodingType::Float64
        Float64Value.from_io(io)
      when EncodingType::String
        StringValue.from_io(io)
      when EncodingType::Bool
        BoolValue.from_io(io)
      when EncodingType::Array
        ArrayValue.from_io(io)
      else
        raise InvalidEncoding.new
      end
    end

    def self.[](created_at : Time, uuid : Int32, value : Int32 | Int64)
      Int64Value.new(created_at, uuid, value.to_i64)
    end

    def self.[](created_at : Time, uuid : Int32, value : Float32 | Float64)
      Float64Value.new(created_at, uuid, value.to_f64)
    end

    def self.[](created_at : Time, uuid : Int32, value : Array)
      ArrayValue.new(created_at, uuid, value.map(&.to_s))
    end

    def self.[](created_at : Time, uuid : Int32, value : Bool)
      BoolValue.new(created_at, uuid, value)
    end

    def self.[](created_at : Time, uuid : Int32, value)
      StringValue.new(created_at, uuid, value.to_s)
    end
  end

  record StringValue, created_at : Time, uuid : Int32, value : String do
    include Value

    def self.from_io(io)
      created_at = Time.epoch(io.read_bytes(Int64))
      uuid = io.read_bytes(Int32)
      value = io.gets(io.read_bytes(Int32)).to_s
      new(created_at, uuid, value)
    end

    def to_io(io)
      io.write_bytes(encoding_type.value)
      io.write_bytes(created_at.epoch)
      io.write_bytes(uuid)
      io.write_bytes(value.size)
      io.write(value.to_slice)
    end

    def encoding_type
      EncodingType::String
    end
  end
  record Int64Value, created_at : Time, uuid : Int32, value : Int64 do
    include Value

    def self.from_io(io)
      created_at = Time.epoch(io.read_bytes(Int64))
      uuid = io.read_bytes(Int32)
      value = io.read_bytes(Int64)
      new(created_at, uuid, value)
    end

    def to_io(io)
      io.write_bytes(encoding_type.value)
      io.write_bytes(created_at.epoch)
      io.write_bytes(uuid)
      io.write_bytes(value)
    end

    def encoding_type
      EncodingType::Int64
    end
  end
  record Float64Value, created_at : Time, uuid : Int32, value : Float64 do
    include Value

    def self.from_io(io)
      created_at = Time.epoch(io.read_bytes(Int64))
      uuid = io.read_bytes(Int32)
      value = io.read_bytes(Float64)
      new(created_at, uuid, value)
    end

    def to_io(io)
      io.write_bytes(encoding_type.value)
      io.write_bytes(created_at.epoch)
      io.write_bytes(uuid)
      io.write_bytes(value)
    end

    def encoding_type
      EncodingType::Float64
    end
  end
  record BoolValue, created_at : Time, uuid : Int32, value : Bool do
    include Value

    def self.from_io(io)
      created_at = Time.epoch(io.read_bytes(Int64))
      uuid = io.read_bytes(Int32)
      value = io.read_bytes(UInt8) == 1
      new(created_at, uuid, value)
    end

    def to_io(io)
      io.write_bytes(encoding_type.value)
      io.write_bytes(created_at.epoch)
      io.write_bytes(uuid)
      io.write_bytes(value ? 1_u8 : 0_u8)
    end

    def encoding_type
      EncodingType::Bool
    end
  end
  record ArrayValue, created_at : Time, uuid : Int32, value : Array(String) do
    include Value

    def self.from_io(io)
      created_at = Time.epoch(io.read_bytes(Int64))
      uuid = io.read_bytes(Int32)
      value = Array(String).new(io.read_bytes(Int32)) do
        io.gets(io.read_bytes(Int32)).to_s
      end
      new(created_at, uuid, value)
    end

    def to_io(io)
      io.write_bytes(encoding_type.value)
      io.write_bytes(created_at.epoch)
      io.write_bytes(uuid)
      io.write_bytes(value.size)
      value.each do |string|
        io.write_bytes(string.size)
        io.write(string.to_slice)
      end
    end

    def encoding_type
      EncodingType::Array
    end
  end
end
