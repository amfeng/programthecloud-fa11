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
