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

    table :xget_responses, [:xid, :key, :reqid] => [:data]
    table :kvgets, [:reqid] => [:key]
  end

  bloom do
    lock_statuses <= lock_status
    xput_responses <= xput_response
    kvputs <= kvput

    xget_responses <= xget_response
    kvgets <= kvget
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
    tick
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

  def test_get
    @kvs.sync_do { @kvs.xput <+ [["1", "foo", "1", "bar"]] }
    tick
    @kvs.sync_do { @kvs.xget <+ [["1", "foo", "1"]] }
    tick

    @kvs.locks.each do |l|
      if l.resource == "foo"
        assert_equal(l.xid, "1")
        assert_equal(l.mode, :X)        
      end
    end    

    @kvs.kvputs.each do |put|
      assert_equal(put.key, "foo")
    end
    
    @kvs.get_queue.each do |get|
      assert_equal(get.key, "foo")
    end
    
    @kvs.kvstate.each do |kv|
      assert_equal(kv.key, "foo")
      assert_equal(kv.value, "bar")
    end

    # assert_equal(@kvs.kvget_response.length, 1)

    assert_equal(@kvs.xget_responses.length, 1)
    
    assert_equal(@kvs.kvgets.length, 1)

    @kvs.kvgets.each do |get|
      assert_equal(get.key, "foo")
    end
  end
end


