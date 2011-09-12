require 'rubygems'
require 'bud'
require 'fifo'
require 'test/unit'

class FC
  include Bud
  include FIFODelivery

  state do
    table :timestamped, [:time, :ident, :payload]
  end

  bloom do
    timestamped <= pipe_chan {|c| [budtime, c.ident, c.payload]}
    #stdio <~ [[timestamped.length]]
  end

end

class TestFIFO < Test::Unit::TestCase
  def workload(fd)
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:54321", 3, "qux"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:54321", 1, "bar"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:54321", 0, "foo"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:54321", 2, "baz"] ] }
  end

  def test_fifo
    sender_instance = FC.new(:port => 54321)
    receiver_instance = FC.new(:port => 12345)

    sender_instance.run_bg
    receiver_instance.run_bg
    workload(sender_instance)
    4.times {receiver_instance.sync_do}
    receiver_instance.timestamped.each do |t|
      receiver_instance.timestamped.each do |t2|
        if t.ident < t2.ident
          assert(t.time < t2.time)
        end
      end
    end
    assert_equal(4, receiver_instance.timestamped.length)
  end
end
