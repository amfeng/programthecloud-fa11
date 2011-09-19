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
  def test_lockmgr
    lm = Locker.new()

    lm.run_bg
    lm.sync_do { lm.request_lock <+ [ ["1", "A", "S"] ] }
    lm.sync_do { lm.request_lock <+ [ ["2", "B", "X"] ] }
    2.times {lm.sync_do}

    lm.acquired_locks.each do |l|
      if l.resource == "A"
        assert_equal(l.xid, "1")
        assert_equal(l.mode, "S")
      end

      if l.resource == "B"
        assert_equal(l.xid, "2")
        assert_equal(l.mode, "X")        
      end
    end
  end
end

