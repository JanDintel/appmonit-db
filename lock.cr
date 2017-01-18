database = Appmonit::DB::Database.new("my-prpject")
database.write_events([] of Event)
database.read_events("my-collection", start_time, end_time)
