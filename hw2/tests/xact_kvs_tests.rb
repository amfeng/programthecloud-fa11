require 'rubygems'
require 'bud'
require '../xact_kvs'
require 'test/unit'

class KVS
  include Bud
  include TwoPLTransactionalKVS 

  state do
    table :lock_statuses, [:xid, :resource] => [:status]
    table :xput_responses, [:xid, :key, :reqid]
    table :kvputs, [:client, :key] => [:reqid, :value]
  end

  bloom do
    lock_statuses <= lock_status
    xput_responses <= xput_response
    kvputs <= kvput
  end

end

class TestKVS < Test::Unit::TestCase
  @kvs = nil
  def setup
    @kvs = KVS.new()
    @kvs.run_bg
  end

  def tick(n = 4)
    n.times { @kvs.sync_do }
  end

  def test_put

    @kvs.sync_do { @kvs.xput <+ [["1", "foo", "1", "bar"]] }
    tick(20)
    @kvs.locks.each do |l|
      if l.resource == "foo"
        assert_equal(l.xid, "1")
        assert_equal(l.mode, :X)        
      end
    end

    @kvs.lock_statuses.each do |l|
      assert_equal(l.xid, "1")
      assert_equal(l.resource, "foo")
    end

    @kvs.kvputs.each do |put|
      assert_equal(put.key, "foo")
    end

    @kvs.xput_responses.each do |responses|
      assert_equal(responses.xid, "1")
    end
    
    assert_equal(@kvs.kvstate.length, 1)
  end
end


