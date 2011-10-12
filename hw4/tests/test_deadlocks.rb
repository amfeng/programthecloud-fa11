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
    deadlocks = d.sync_callback(:add_link,  [["a", "b"], ["b", "c"], ["c", "d"], ["d", "a"]], :deadlock)
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

  # def tick(n = 1)
  #   n.times { @lm.sync_do }
  # end

  def test_distributed_cycle
    master = DistributedDeadlockDetectorTest.new(:port=>54320)
    master.run_bg
    node1 = DistributedDeadlockDetectorTest.new(:port=>54321)
    node1.run_bg
    node2 = DistributedDeadlockDetectorTest.new(:port=>54322)
    node2.run_bg

    node1.sync_do {node1.set_coordinator <+ [[master]]}
    node2.sync_do {node2.set_coordinator <+ [[master]]}

    node1.sync_do { node1.request_lock <+ [ ["1", "A", :X] ] }
    node1.sync_do {}
    # FIXME: When I uncomment these lines - the test case fails. Why?
    # node1.sync_do { node1.request_lock <+ [ ["2", "A", :X] ] }
    # node1.sync_do {}

    node2.sync_do { node2.request_lock <+ [ ["2", "A", :X] ] }
    node2.sync_do {}

    node1.locks.each do |l|
      if l.resource == "A"
        assert_equal(l.xid, "1")
        assert_equal(l.mode, :X)
      end
    end
    assert_equal(node1.locks.length, 1)
    # FIXME: Shouldn't this be 0? But it is 1
    assert_equal(node2.locks.length, 1)

    # node1.sync_do { node2.request_lock <+ [ ["2", "B", :X] ] }
    # node1.sync_do { node2.request_lock <+ [ ["1", "A", :X] ] }

    # d = LocalDeadlockDetectorTest.new
    # deadlocks = d.sync_callback(:add_link,  [["a", "b"], ["b", "c"], ["c", "d"], ["d", "a"]], :deadlock)
    # assert_equal(1, deadlocks.length)
    # assert_equal(["a", "b", "c", "d"], deadlocks.first[0])
    # assert_equal("d", deadlocks.first[1])
  end
end
