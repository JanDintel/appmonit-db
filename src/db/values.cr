module Appmonit::DB
  class InvalidValueError < Exception
  end

  class Values < Array(Value)
    def self.create(encoding_type : EncodingType, size : Int32) : ValuesType
      case encoding_type
      when EncodingType::Int64
        Int64Values.new(size)
      when EncodingType::Float64
        Float64Values.new(size)
      when EncodingType::String
        StringValues.new(size)
      when EncodingType::Bool
        BoolValues.new(size)
      when EncodingType::Array
        ArrayValues.new(size)
      else
        raise InvalidEncoding.new
      end
    end

    def self.from_io(io) : Values
      size = io.read_bytes(Int32)
      values = Values.new(size)
      size.times do
        values << Value.from_io(io)
      end
      values
    end

    def to_io(io)
      io.write_bytes(size)
      each do |value|
        value.to_io(io)
      end
    end
  end

  {% for type in ["Float64", "Int64", "Bool", "String", "Array"] %}
    class {{type.id}}Values < Array({{type.id}}Value)
      def <<(value)
        raise InvalidValueError.new("Cannot set #{value.class} to #{self.class}")
      end

      def <<(value : {{type.id}}Value)
        super
      end

      def encoding_type
        EncodingType::{{type.id}}
      end

      def sort
        sort_by(&.created_at)
      end

      def to_io(io)
        io.write_bytes(encoding_type)
        io.write_bytes(size)
        self.each do |value|
          value.to_io(io)
        end
      end
    end
  {% end %}
end
