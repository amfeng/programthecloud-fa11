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

    # Populate kvstate with two key-values
    @kvs.sync_callback(:xput, [["A", "foo", 1, "bar"]], :xput_response)
    @kvs.sync_callback(:xput, [["A", "foo2", 1, "baz"]], :xput_response)
    @kvs.sync_do { @kvs.end_xact <+ [["A"]] }
    tick

    assert_equal(@kvs.locks.length, 0)
    # T1 does a get and a put on "foo"
    @kvs.sync_callback(:xget, [["T1", "foo", 2]], :xget_response)
    @kvs.sync_callback(:xput, [["T1", "foo", 3, "foo_a"]], :xput_response)
    tick
    
    # T1 should have obtained a :X lock on "foo"
    assert_equal(@kvs.locks.length, 1)
    @kvs.locks.each do |l|
      if l.resource == "foo"
        assert_equal(l.xid, "T1")
        assert_equal(l.mode, :X)        
      end
    end
    
    # kvstate should have the updated value for "foo" but the old value for "foo2"
    assert_equal(@kvs.kvstate.length, 2)
    @kvs.kvstate.each do |kv|
      if (kv.key == "foo")
        assert_equal(kv.value, "foo_a")        
      end
      if (kv.key == "foo2")
        assert_equal(kv.value, "baz")
      end
    end

    # T2 tries to do a get and put for "foo"
    @kvs.sync_do { @kvs.xget <+ [["T2", "foo", "4"]] }
    @kvs.sync_do { @kvs.xput <+ [["T2", "foo", "5", "foo_b"]] }

    
    # T2 should be blocked, since T1 has a :X lock on "foo"
    assert_equal(@kvs.locks.length, 1)
    @kvs.locks.each do |l|
      if l.resource == "foo"
        assert_equal(l.xid, "T1")
        assert_equal(l.mode, :X)        
      end
    end

    # T1 does a get and put on "foo2"
    @kvs.sync_callback(:xget, [["T1", "foo2", 6]], :xget_response)
    @kvs.sync_callback(:xput, [["T1", "foo2", 7, "foo2_a"]], :xput_response)
    tick(1)

    # T1 should have a :X lock on "foo" and "foo2"
    assert_equal(@kvs.locks.length, 2)
    @kvs.locks.each do |l|
      if l.resource == "foo"
        assert_equal(l.xid, "T1")
        assert_equal(l.mode, :X)        
      end
      if l.resource == "foo2"
        assert_equal(l.xid, "T1")
        assert_equal(l.mode, :X)        
      end
    end

    # kvstate should have the T1's updated values for "foo" and "foo2"
    assert_equal(@kvs.kvstate.length, 2)
    @kvs.kvstate.each do |kv|
      if (kv.key == "foo")
        assert_equal(kv.value, "foo_a")        
      end
      if (kv.key == "foo2")
        assert_equal(kv.value, "foo2_a")
      end
    end

    # End T1
    @kvs.sync_do { @kvs.end_xact <+ [["T1"]] }
    tick

    # Since T2 had been trying to do a get and put on "foo" - it should obtain a :X lock for "foo" now
    assert_equal(@kvs.locks.length, 1)
    @kvs.locks.each do |l|
      if l.resource == "foo"
        assert_equal(l.xid, "T2")
        assert_equal(l.mode, :X)        
      end
    end
    
    # kvstate should have T2's value for "foo" and T1's for "foo2"
    assert_equal(@kvs.kvstate.length, 2)
    @kvs.kvstate.each do |kv|
      if (kv.key == "foo")
        assert_equal(kv.value, "foo_b")        
      end
      if (kv.key == "foo2")
        assert_equal(kv.value, "foo2_a")
      end
    end
    
    # T2 does a get and put on "foo2"
    @kvs.sync_callback(:xput, [["T2", "foo2_b", 5, "foo2_b"]], :xput_response)
    @kvs.sync_callback(:xget, [["T2", "foo2_b", 4]], :xget_response)
    tick

    # T2 should have a :X lock on "foo" and "foo2"
    assert_equal(@kvs.locks.length, 2)
    @kvs.locks.each do |l|
      if l.resource == "foo"
        assert_equal(l.xid, "T2")
        assert_equal(l.mode, :X)        
      end
      if l.resource == "foo2"
        assert_equal(l.xid, "T2")
        assert_equal(l.mode, :X)        
      end
    end
    
    # kvstate should have T2's updated values for "foo" and "foo2" and "foo2_b"
    assert_equal(@kvs.kvstate.length, 3)
    @kvs.kvstate.each do |kv|
      if (kv.key == "foo")
        assert_equal(kv.value, "foo_b")        
      end
      if (kv.key == "foo2")
        assert_equal(kv.value, "foo2_a")
      end
      if (kv.key == "foo2_b")
        assert_equal(kv.value, "foo2_b")
      end
    end

    # End T2
    @kvs.sync_do { @kvs.end_xact <+ [["T2"]] }
    tick
    
    # All locks should have been released
    assert_equal(@kvs.locks.length, 0)
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

  def test_concurrent_accesses
    # Populate kvstate with two key-values
    @kvs.sync_callback(:xput, [["T", "A", 1, 1000]], :xput_response)
    @kvs.sync_callback(:xput, [["T", "B", 2, 1000]], :xput_response)
    @kvs.sync_do { @kvs.end_xact <+ [["T"]] }
    tick
    assert_equal(@kvs.locks.length, 0)
    
    # T1 does a get and a put on "foo"
    @kvs.sync_do { @kvs.xput <+ [["T2", "A", 3, 1060], ["T1", "A", 4, 1100], ["T1", "B", 5, 900], ["T2", "B", 6, 960]] }
    @kvs.sync_do
    assert_equal(@kvs.locks.length, 2)
    assert_equal(@kvs.kvstate.length, 2)
    
    @kvs.kvstate.each do |kv|
      @kvs.locks.each do |l|
        if kv.key == "A" and l.xid == "T1" and l.resource == "A"
          assert_equal(kv.value, 1100)
        end
        if kv.key == "A" and l.xid == "T2" and l.resource == "A"
          assert_equal(kv.value, 1060)
        end
        if kv.key == "B" and l.xid == "T1" and l.resource == "B"
          assert_equal(kv.value, 900)
        end
        if kv.key == "B" and l.xid == "T2" and l.resource == "B"
          assert_equal(kv.value, 960)
        end
      end
    end

  end
end


