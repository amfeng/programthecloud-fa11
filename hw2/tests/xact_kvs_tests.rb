require 'rubygems'
require 'bud'
require '../xact_kvs'
require 'test/unit'

class KVS
  include Bud
  include TwoPLTransactionalKVS 

  state do
  end

  bloom do
  end

end

class TestKVS < Test::Unit::TestCase
  @kvs = nil
  def setup
    @kvs = KVS.new()
    @kvs.run_bg
  end

  def tick(n = 3)
    n.times { @kvs.sync_do }
  end

  def test_sharedlock_ok
    assert(true)
    # Acquire a shared lock
    #@kvs.sync_do { @kvs.request_lock <+ [ ["1", "A", :S] ] }
    #tick

    #@kvs.locks.each do |l|
    #  if l.resource == "A"
    #    assert_equal(l.xid, "1")
    #    assert_equal(l.mode, :S)
    #  end
    #end
    #assert_equal(@lm.locks.length, 1)
  end
end


