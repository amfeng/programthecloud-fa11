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

    # Multiple Xacts can acquire a shared lock on a resource
    # lm.sync_do { lm.request_lock <+ [ ["4", "C", "S"] ] }
    # lm.sync_do { lm.request_lock <+ [ ["5", "C", "S"] ] }
    # 2.times {lm.sync_do}

    # acquiredLocks = Array.new
    # lm.acquired_locks.each do |l|
    #   if l.resource == "C"
    #     assert_equal(l.mode, "S")        
    #     assert(["4", "5"].include?(l.xid))        
    #     acquiredLocks << l.xid
    #   end
    # end
    # acquiredLocks.sort!
    # assert_equals(acquiredLocks.at(0), "4")
    # assert_equals(acquiredLocks.at(1), "5")
    # assert_equals(acquiredLocks.length, 2)

    # TODO: Clarify if a Xact has an exclusive lock, can it acquire
    # a shared lock. Will that result in a downgrade?
    
    # Trying to acquire a :S lock on a resource when 
    # the same Xact already has a :X lock on it
    # lm.sync_do { lm.request_lock <+ [ ["6", "D", "X"] ] }
    # lm.sync_do { lm.request_lock <+ [ ["6", "D", "S"] ] }
    # 2.times {lm.sync_do}

    # lm.acquired_locks.each do |l|
    #   if l.resource == "D"
    #     assert_equal(l.xid, "6")
    #     assert_equal(l.mode, "X")        
    #   end
    # end

    # Lock upgrade
    # lm.sync_do { lm.request_lock <+ [ ["7", "E", "S"] ] }
    # 2.times {lm.sync_do}

    # lm.acquired_locks.each do |l|
    #   if l.resource == "E"
    #     assert_equal(l.xid, "7")
    #     assert_equal(l.mode, "S")        
    #   end
    # end

    # lm.sync_do { lm.request_lock <+ [ ["7", "E", "X"] ] }
    # 2.times {lm.sync_do}

    # lm.acquired_locks.each do |l|
    #   if l.resource == "E"
    #     assert_equal(l.xid, "7")
    #     assert_equal(l.mode, "X")        
    #   end
    # end

    # Lock upgrade cannot happen if multiple Xacts have a :S lock
    # lm.sync_do { lm.request_lock <+ [ ["8", "F", "S"] ] }
    # lm.sync_do { lm.request_lock <+ [ ["9", "F", "S"] ] }
    # lm.sync_do { lm.request_lock <+ [ ["8", "F", "X"] ] }
    # 2.times {lm.sync_do}

    # acquiredLocks = Array.new
    # lm.acquired_locks.each do |l|
    #   if l.resource == "F"
    #     assert_equal(l.mode, "S")        
    #     assert(["8", "9"].include?(l.xid))        
    #     acquiredLocks << l.xid
    #   end
    # end
    # acquiredLocks.sort!
    # assert_equals(acquiredLocks.at(0), "8")
    # assert_equals(acquiredLocks.at(1), "9")
    # assert_equals(acquiredLocks.length, 2)

    # Acquire a :X lock, end Xact, acquire a :S lock
    # lm.sync_do { lm.request_lock <+ [ ["10", "G", "X"] ] }
    # 2.times {lm.sync_do}
    # lm.sync_do { lm.end_xact <+ [ ["10"] ] }
    # lm.sync_do { lm.request_lock <+ [ ["11", "G", "S"] ] }
    # 2.times {lm.sync_do}

    # lm.acquired_locks.each do |l|
    #   if l.resource == "G"
    #     assert_equal(l.mode, "S")        
    #     assert_equal(l.xid, "11")        
    #   end
    # end

    # Acquire many :S locks, end one Xact 
    # Try to acquire a :X lock - fails
    # Try to acquire a :S lock - succeeds
    # lm.sync_do { lm.request_lock <+ [ ["12", "H", "S"] ] }
    # lm.sync_do { lm.request_lock <+ [ ["13", "H", "S"] ] }
    # 2.times {lm.sync_do}
    # lm.sync_do { lm.end_xact <+ [ ["12"] ] }
    # lm.sync_do { lm.request_lock <+ [ ["14", "H", "X"] ] }
    # lm.sync_do { lm.request_lock <+ [ ["15", "H", "S"] ] }
    # 2.times {lm.sync_do}

    # acquiredLocks = Array.new
    # lm.acquired_locks.each do |l|
    #   if l.resource == "H"
    #     assert_equal(l.mode, "S")        
    #     assert(["13", "15"].include?(l.xid))        
    #     acquiredLocks << l.xid
    #   end
    # end

    # acquiredLocks.sort!
    # assert_equals(acquiredLocks.at(0), "13")
    # assert_equals(acquiredLocks.at(1), "15")
    # assert_equals(acquiredLocks.length, 2)


    # End Xact and perform a Lock upgrade
    # lm.sync_do { lm.request_lock <+ [ ["16", "I", "S"] ] }
    # lm.sync_do { lm.request_lock <+ [ ["17", "I", "S"] ] }
    # 2.times {lm.sync_do}
    # lm.sync_do { lm.end_xact <+ [ ["16"] ] }
    # lm.sync_do { lm.request_lock <+ [ ["17", "I", "X"] ] }

    # lm.acquired_locks.each do |l|
    #   if l.resource == "I"
    #     assert_equal(l.xid, "17")
    #     assert_equal(l.mode, "X")        
    #   end
    # end

  end
end

