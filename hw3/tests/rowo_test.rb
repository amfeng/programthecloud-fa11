require 'rubygems'
require 'bud'
require 'test/unit'
require '../quorumkvs'

class TestQuorum < Test::Unit::TestCase
  class QuorumKVSTest 
    include Bud
    include QuorumKVS

    state do
      table :acks, [:reqid]
    end

    bootstrap do
      add_member <= [
        ['localhost:54321', 1],
        ['localhost:54322', 2],
        ['localhost:54323', 3]
      ]
    end

    bloom do
      acks <= kv_acks
      stdio <~ kvput_chan.inspected
    end
  end

  def test_rowo
    p1 = QuorumKVSTest.new(:port=>54321)
    p1.run_bg
    p2 = QuorumKVSTest.new(:port=>54322)
    p2.run_bg
    p3 = QuorumKVSTest.new(:port=>54323)
    p3.run_bg

    # Read all, write all (should be consistent)
    p1.sync_do {p1.quorum_config <+ [[1, 1]]}
    p2.sync_do {p2.quorum_config <+ [[1, 1]]}
    p3.sync_do {p3.quorum_config <+ [[1, 1]]}

    p1.sync_do {p1.kvput <+ [[:joe, 1, :hellerstein]]}
    assert_equal(p1.acks.length, 1)

    p2.sync_do {p2.kvput <+ [[:peter, 2, :alvaro]]}
    assert_equal(p2.acks.length, 1)

    p3.sync_do {p3.kvput <+ [[:joe, 3, :piscopo]]}
    assert_equal(p3.acks.length, 1)

    p3.sync_do {p3.kvput <+ [[:peter, 4, :tosh]]}
    assert_equal(p3.acks.length, 1)

    resps = p1.sync_callback(p1.kvget.tabname, [[5, :joe]], p1.kvget_response.tabname)
    assert_equal([[5, "joe", "piscopo"]], resps)

    resps = p3.sync_callback(p1.kvget.tabname, [[6, :joe]], p1.kvget_response.tabname)
    assert_equal([[6, "joe", "piscopo"]], resps)

    resps = p1.sync_callback(p1.kvget.tabname, [[7, :peter]], p1.kvget_response.tabname)
    assert_equal([[7, "peter", "tosh"]], resps)

    resps = p3.sync_callback(p3.kvget.tabname, [[8, :peter]], p1.kvget_response.tabname)
    assert_equal([[8, "peter", "tosh"]], resps)

    p1.stop
    p2.stop
    p3.stop(true, true)
  end
end
