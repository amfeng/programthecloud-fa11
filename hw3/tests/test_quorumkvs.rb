require 'rubygems'
require 'bud'
require 'test/unit'
require '../quorum_kvs'

class TestQuorum < Test::Unit::TestCase
  class QuorumKVSTest 
    include Bud
    include QuorumKVS

    bootstrap do
      add_member <= [
        ['localhost:54320', 0],
        ['localhost:54321', 1],
        ['localhost:54322', 2],
        ['localhost:54323', 3],
        ['localhost:54324', 4]
      ]
    end
  end

  def test_quorum
    p1 = QuorumKVSTest.new(:port=>54320)
    p1.run_bg
    p2 = QuorumKVSTest.new(:port=>54321)
    p2.run_bg
    p3 = QuorumKVSTest.new(:port=>54322)
    p3.run_bg
    p4 = QuorumKVSTest.new(:port=>54323)
    p4.run_bg
    p5 = QuorumKVSTest.new(:port=>54324)
    p5.run_bg

    # Read all, write all (should be consistent)
    p1.sync_do {p1.quorum_config <+ [[0, 0]]}
    p2.sync_do {p2.quorum_config <+ [[0, 0]]}
    p3.sync_do {p3.quorum_config <+ [[0, 0]]}
    p4.sync_do {p4.quorum_config <+ [[0, 0]]}
    p5.sync_do {p5.quorum_config <+ [[0, 0]]}
    
    # Insert a key-value and read from the same machine
    acks = p1.sync_do {p1.kvput <+ [[:A, :anirudh, 1, :todi]]}
    resps = p1.sync_callback(:kvget, [[2, :anirudh]], :kvget_response)
    assert_equal([[2, "anirudh", "todi"]], resps)

    # Insert a key-value and read from a different machine
    acks = p2.sync_do {p2.kvput <+ [[:A, :amber, 3, :feng]]}
    resps = p3.sync_callback(:kvget, [[4, :amber]], :kvget_response)
    assert_equal([[4, "amber", "feng"]], resps)

    # Overwrite a previously inserted key-value
    acks = p2.sync_do {p2.kvput <+ [[:A, :anirudh, 5, :upe]]}
    resps = p1.sync_callback(p1.kvget.tabname, [[6, :anirudh]], p1.kvget_response.tabname)
    assert_equal([[6, "anirudh", "upe"]], resps)

    # # Delete a previously inserted key-value - CHECK
    # acks = p3.sync_do {p1.kvdel <+ [[:amber, 7]]}
    # resps = p2.sync_callback(p1.kvget.tabname, [[8, :amber]], p1.kvget_response.tabname)
    # assert_equal([[8, "amber", nil]], resps)

    # Check that the value we had inserted long ago is still there and accessible
    resps = p2.sync_callback(p3.kvget.tabname, [[11, :anirudh]], p3.kvget_response.tabname)
    assert_equal([[11, "anirudh", "upe"]], resps)

    # Insert a value for a previously deleted key-value
    acks = p2.sync_do {p2.kvput <+ [[:A, :amber, 9, :hkn]]}
    # resps = p2.sync_callback(p3.kvget.tabname, [[10, :amber]], p3.kvget_response.tabname)
    resps = p2.sync_callback(:kvget, [[10, :amber]], :kvget_response)
    assert_equal([[10, "amber", "hkn"]], resps)


    p1.stop
    p2.stop
    p3.stop
    p4.stop
    p5.stop(true, true)
  end
end
