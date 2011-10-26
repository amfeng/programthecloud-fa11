require 'rubygems'
require 'bud'
require 'test/unit'
require '../deadlock'
require '../lckmgr'

class TestDeadlocks < Test::Unit::TestCase
  class LocalDeadlockDetectorTest
    include Bud
    include TwoPhaseLockMgr
    include LocalDeadlockDetector
  end

  def test_detect_cycle
    d = LocalDeadlockDetectorTest.new
    deadlocks = d.sync_callback(:add_link,  [["a", "b"], ["b", "c"], 
                                             ["c", "d"], ["d", "a"]], :deadlock)
    assert_equal(1, deadlocks.length)
    assert_equal(["a", "b", "c", "d"], deadlocks.first[0])
    assert_equal("d", deadlocks.first[1])
  end
end

class TestDistributedDeadlocks < Test::Unit::TestCase
  class DistributedDeadlockDetectorTest
    include Bud
    include TwoPhaseLockMgr
    include LocalDeadlockDetector
    include DDLNode
    include DDLMaster
  end

  def test_distributed_cycle
    master = DistributedDeadlockDetectorTest.new(:port=>54320)
    master.run_bg
    node1 = DistributedDeadlockDetectorTest.new(:port=>54321)
    node1.run_bg
    node2 = DistributedDeadlockDetectorTest.new(:port=>54322)
    node2.run_bg

    node1.sync_do {node1.set_coordinator <+ [["localhost:54320"]]}
    node2.sync_do {node2.set_coordinator <+ [["localhost:54320"]]}

    node1.sync_do {}
    node2.sync_do {}
    master.sync_do {}

    master.register_callback(:deadlock) do |cb|
      cb.each do |row|
        assert_equal('4', row.victim)
      end
    end

    # xact 1 is waiting for 2
    node1.sync_do { node1.request_lock <+ [ ["2", "A", :X] ] }
    node1.sync_do {}
    node1.sync_do { node1.request_lock <+ [ ["1", "A", :X] ] }
    node1.sync_do {}

    # xact 2 is waiting for 3
    node1.sync_do { node1.request_lock <+ [ ["3", "B", :X] ] }
    node1.sync_do {}
    node1.sync_do { node1.request_lock <+ [ ["2", "B", :X] ] }
    node1.sync_do {}

    node1.locks.each do |l|
      if l.resource == "A"
        assert_equal(l.xid, "2")
        assert_equal(l.mode, :X)
      end
      if l.resource == "B"
        assert_equal(l.xid, "3")
        assert_equal(l.mode, :X)
      end
    end
    assert_equal(node1.locks.length, 2)

    # xact 3 is waiting for 4
    node2.sync_do { node2.request_lock <+ [ ["4", "C", :X] ] }
    node2.sync_do {}
    node2.sync_do { node2.request_lock <+ [ ["3", "C", :X] ] }
    node2.sync_do {}

    # xact 4 is waiting for 1
    node2.sync_do { node2.request_lock <+ [ ["1", "D", :X] ] }
    node2.sync_do {}
    node2.sync_do { node2.request_lock <+ [ ["4", "D", :X] ] }
    node2.sync_do {}

    node2.locks.each do |l|
      if l.resource == "C"
        assert_equal(l.xid, "4")
        assert_equal(l.mode, :X)
      end
      if l.resource == "D"
        assert_equal(l.xid, "1")
        assert_equal(l.mode, :X)
      end
    end
    assert_equal(node2.locks.length, 2)
  end
end
