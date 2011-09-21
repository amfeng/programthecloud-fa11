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
    table :kvputs, [:client, :key, :reqid, :value]

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

  def teardown
    @kvs.stop_bg
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

  # Testing a conflict serializable schedule
  def test_conflict_serialiability

    @kvs.register_callback(:xput_response) do |cb|
      cb.each do |row|
        assert(["A", "T1"].include? row.xid)
        assert_equal("foo", row.key)
      end 
    end

    @kvs.register_callback(:xget_response) do |cb|
      cb.each do |row|
        assert_equal("T1", row.xid)
        assert_equal("bar", row.data)
      end 
    end
        
    @kvs.sync_callback(:xput, [["A", "foo", 1, "bar"]], :xput_response)
    @kvs.sync_do { @kvs.end_xact <+ [["A"]] }
    
    @kvs.sync_callback(:xget, [["T1", "foo", 2]], :xget_response)
    @kvs.sync_callback(:xput, [["T1", "foo", 3, "baz"]], :xput_response)

    @kvs.locks.each do |l|
      if l.resource == "foo"
        assert_equal(l.xid, "T1")
        assert_equal(l.mode, :X)        
      end
    end

    
    # @kvs.sync_callback(:xget, [["A", "foo", 1]], :xget_response)

    # @kvs.sync_callback(:xput, [["A", "foo", 8, "big"]], :xput_response)
    # res = @kvs.sync_callback(:xget, [["A", "foo", 9]], :xget_response)

    # @kvs.sync_do { @kvs.xget <+ [["B", "foo", 10]] }
    # @kvs.sync_do { @kvs.xget <+ [["C", "foo", 11]] }
    # @kvs.sync_do { @kvs.xget <+ [["D", "foo", 12]] }
    # @kvs.sync_do { @kvs.end_xact <+ [["A"]] }
    # tick
  end

  # From bud-sandbox KVS tests
  def test_simple_kvs
    workload1
    @kvs.sync_do { assert_equal(1, @kvs.kvstate.length) }
    @kvs.sync_do { assert_equal("bak", @kvs.kvstate.first[1]) }

    @kvs.sync_do { @kvs.end_xact <+ [["localhost:54321"]] }
    @kvs.sync_do { @kvs.xget <+ [[1234, 'foo', 5]] }
    @kvs.sync_do {
      assert_equal(1, @kvs.xget_response.length)
      assert_equal("bak", @kvs.xget_response.first[3])
    }

  end

  def workload1()
    @kvs.sync_do { @kvs.xput <+ [["localhost:54321", "foo", 1, "bar"]] }
    @kvs.sync_do { @kvs.xput <+ [["localhost:54321", "foo", 2, "baz"]] }
    @kvs.sync_do { @kvs.xput <+ [["localhost:54321", "foo", 3, "bam"]] }
    @kvs.sync_do { @kvs.xput <+ [["localhost:54321", "foo", 4, "bak"]] }
  end

  # From bud-sandbox KVS tests
  def test_xact_kvs
    @kvs.register_callback(:xget_response) do |cb|
      cb.each do |row|
        assert(["A", "B", "C", "D"].include? row.xid)
        assert_equal("big", row.data)
      end 
    end

    @kvs.sync_callback(:xput, [["A", "foo", 6, "bar"]], :xput_response)
    @kvs.sync_callback(:xput, [["A", "bam", 7, "biz"]], :xput_response)
    @kvs.sync_callback(:xput, [["A", "foo", 8, "big"]], :xput_response)
    res = @kvs.sync_callback(:xget, [["A", "foo", 9]], :xget_response)

    @kvs.sync_do { @kvs.xget <+ [["B", "foo", 10]] }
    @kvs.sync_do { @kvs.xget <+ [["C", "foo", 11]] }
    @kvs.sync_do { @kvs.xget <+ [["D", "foo", 12]] }
    @kvs.sync_do { @kvs.end_xact <+ [["A"]] }
    tick
  end
end


