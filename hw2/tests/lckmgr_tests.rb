require 'rubygems'
require 'bud'
require '../lckmgr'
require 'test/unit'

class Locker 
  include Bud
  include TwoPhaseLockMgr 

  state do
    table :acquired_locks , [:time, :xid, :resource, :mode]
  end

  bloom do
    acquired_locks <= locks {|r| [budtime, r.xid, r.resource, r.mode]}
  end

end

class TestLockMgr < Test::Unit::TestCase
  def workload(fd)
    fd.sync_do { fd.request_lock <+ [ ["1", "A", "S"] ] }
  end

  def test_lockmgr
    lm = Locker.new()

    lm.run_bg
    workload(lm)
    2.times {lm.sync_do}

    lm.acquired_locks.each do |l|
      assert_equal(l.xid, "1")
      assert_equal(l.resource, "A")
      assert_equal(l.mode, "S")
    end
  end
end

