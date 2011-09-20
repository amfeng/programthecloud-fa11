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

    # @kvs.sync_callback(:xput, [[1, "foo", "bar"]], :xput_response)
    # @kvs.sync_callback(:xput, [[1, "bam", "biz"]], :xput_response)
    # @kvs.sync_callback(:xput, [[1, "foo", "big"]], :xput_response)
    # res = @kvs.sync_callback(:xget, [[1, "foo"]], :xget_response)

    # @kvs.register_callback(:xget_response) do |cb|
    #   cb.each do |row|
    #     assert_equal(1, row.xid)
    #     assert_equal("big", row.value)
    #   end 
    # end

    # @kvs.sync_do { @kvs.xget <+ [[2, "foo"]] }

    # @kvs.sync_do { @kvs.end_xact <+ [[1]] }

    # @kvs.register_callback(:xget_response) do |cb|
    #   cb.each do |row|
    #     assert_equal(2, row.xid)
    #     assert_equal("big", row.value)
    #   end 
    # end
  end
end


