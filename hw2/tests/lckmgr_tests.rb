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

    # Acquire a shared lock
    lm.sync_do { lm.request_lock <+ [ ["1", "A", "S"] ] }
    2.times {lm.sync_do}

    lm.acquired_locks.each do |l|
      if l.resource == "A"
        assert_equal(l.xid, "1")
        assert_equal(l.mode, "S")
      end
    end

    # Acquire an exclusive lock
    lm.sync_do { lm.request_lock <+ [ ["2", "B", "X"] ] }
    2.times {lm.sync_do}

    lm.acquired_locks.each do |l|
      if l.resource == "B"
        assert_equal(l.xid, "2")
        assert_equal(l.mode, "X")        
      end
    end

    # Trying to acquire a :X lock on a resource when 
    # another Xact has a :S lock on it
    # lm.sync_do { lm.request_lock <+ [ ["3", "A", "X"] ] }
    # 2.times {lm.sync_do}

    # lm.acquired_locks.each do |l|
    #   if l.resource == "A"
    #     assert_equal(l.xid, "1")
    #     assert_equal(l.mode, "S")        
    #   end
    # end

    # Trying to acquire a :S lock on a resource that 
    # another Xact has a :X lock on
    # lm.sync_do { lm.request_lock <+ [ ["3", "B", "S"] ] }
    # 2.times {lm.sync_do}

    # lm.acquired_locks.each do |l|
    #   if l.resource == "B"
    #     assert_equal(l.xid, "2")
    #     assert_equal(l.mode, "X")        
    #   end
    # end

    # TODO: Clarify if a Xact has an exclusive lock, can it acquire
    # a shared lock. Will that result in a downgrade?
    
    # Trying to acquire a :S lock on a resource when 
    # the same Xact already has a :X lock on it
    # lm.sync_do { lm.request_lock <+ [ ["2", "B", "S"] ] }
    # 2.times {lm.sync_do}

    # lm.acquired_locks.each do |l|
    #   if l.resource == "B"
    #     assert_equal(l.xid, "2")
    #     assert_equal(l.mode, "X")        
    #   end
    # end

    # Lock upgrade
    # lm.sync_do { lm.request_lock <+ [ ["1", "A", "X"] ] }
    # 2.times {lm.sync_do}

    # lm.acquired_locks.each do |l|
    #   if l.resource == "A"
    #     assert_equal(l.xid, "1")
    #     assert_equal(l.mode, "X")        
    #   end
    # end
  end
end

