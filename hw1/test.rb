require 'rubygems'
require 'bud'
require 'fifo'
require 'test/unit'

class FC
  include Bud
  include FIFODelivery

  state do
    table :timestamped, [:time, :src, :ident, :payload]
  end

  bloom do
    timestamped <= pipe_chan {|c| [budtime, c.src, c.ident, c.payload]}
  end

end

class TestFIFO < Test::Unit::TestCase
  def workload(fd)
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:54321", 3, "qux"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:54321", 1, "bar"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:54321", 0, "foo"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:54321", 2, "baz"] ] }
  end

  def workload2(fd)
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:33333", 2, "qux"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:33333", 0, "bar"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:33333", 1, "foo"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:33333", 3, "baz"] ] }
  end

  def workload3(fd)
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:11111", 1, "qux"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:11111", 3, "bar"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:11111", 2, "foo"] ] }
    fd.sync_do { fd.pipe_in <+ [ ["localhost:12345", "localhost:11111", 0, "baz"] ] }
  end

  def test_fifo
    sender_instance = FC.new(:port => 54321)
    sender_instance_2 = FC.new(:port => 33333)
    sender_instance_3 = FC.new(:port => 11111)
    receiver_instance = FC.new(:port => 12345)

    sender_instance.run_bg
    sender_instance_2.run_bg
    sender_instance_3.run_bg
    receiver_instance.run_bg
    workload(sender_instance)
    workload2(sender_instance_2)
    workload3(sender_instance_2)
    4.times {receiver_instance.sync_do}

    receiver_instance.timestamped.each do |t|
      receiver_instance.timestamped.each do |t2|
        # Check ordering is correct
        if t.ident < t2.ident and t.src == t2.src
          assert(t.time < t2.time)
        end

        # Check only one packet per sender at a time
        if t != t2 and t.time == t2.time 
          assert(t.src != t2.src)
        end
      end
    end
    
    # Check number of packets received is correct
    assert_equal(12, receiver_instance.timestamped.length)
  end
end
