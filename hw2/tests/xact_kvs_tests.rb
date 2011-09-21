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
    table :kvget_responses, [:reqid] => [:key, :value]
  end

  bloom do
    lock_statuses <= lock_status
    xput_responses <= xput_response
    kvputs <= kvput

    xget_responses <= xget_response
    kvget_responses <= kvget_response
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

  def test_basic_put
    @kvs.sync_do { @kvs.xput <+ [["A", "foo", "1", "bar"]] }
    tick

    assert_equal(@kvs.locks.length, 1)
    @kvs.locks.each do |l|
      if l.resource == "foo"
        assert_equal(l.xid, "A")
        assert_equal(l.mode, :X)        
      end
    end

    assert_equal(@kvs.lock_statuses.length, 1)

    assert_equal(@kvs.kvputs.length, 1)
    @kvs.kvputs.each do |put|
      assert_equal(put.reqid, "1")
    end

    assert_equal(@kvs.xput_responses.length, 1)
    @kvs.xput_responses.each do |responses|
      assert_equal(responses.xid, "A")
      assert_equal(responses.key, "foo")
      assert_equal(responses.reqid, "1")
    end
    
    assert_equal(@kvs.kvstate.length, 1)
    @kvs.kvstate.each do |kv|
      assert_equal(kv.key, "foo")
      assert_equal(kv.value, "bar")
    end
  end

  def test_basic_get
    @kvs.sync_do { @kvs.xput <+ [["A", "foo", "1", "bar"]] }
    @kvs.sync_do { @kvs.xget <+ [["A", "foo", "1"]] }
    tick

    assert_equal(@kvs.locks.length, 1)
    @kvs.locks.each do |l|
      if l.resource == "foo"
        assert_equal(l.xid, "A")
        assert_equal(l.mode, :X)        
      end
    end    

    assert_equal(@kvs.kvstate.length, 1)
    @kvs.kvstate.each do |kv|
      assert_equal(kv.key, "foo")
      assert_equal(kv.value, "bar")
    end

    assert_equal(@kvs.kvgets.length, 1)
    @kvs.kvgets.each do |put|
      assert_equal(put.reqid, "1")
    end

    assert_equal(@kvs.xget_responses.length, 1)
    @kvs.xget_responses.each do |responses|
      assert_equal(responses.xid, "A")
      assert_equal(responses.key, "foo")
      assert_equal(responses.reqid, "1")
      assert_equal(responses.data, "bar")
    end

    assert_equal(@kvs.get_queue.length, 0)
    

    @kvs.kvgets.each do |get|
      assert_equal(get.key, "foo")
    end
  end
end


