guard 'process', :name => 'Format', :command => 'crystal tool format' do
  watch(/spec\/(.*).e?cr$/)
  watch(/src\/(.*).e?cr$/)
  watch(/lib\/(.*).e?cr$/)
end

guard 'process', :name => 'Spec', :command => 'crystal spec' do
  watch(/spec\/(.*).e?cr$/)
  watch(/src\/(.*).e?cr$/)
  watch(/lib\/(.*).e?cr$/)
end
