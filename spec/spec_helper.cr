require "spec"
require "../src/appmonit-db"

Spec.before_each do
  FileUtils.rm_r("/tmp/appmonit-db") if Dir.exists?("/tmp/appmonit-db")
  Dir.mkdir_p("/tmp/appmonit-db")
end
