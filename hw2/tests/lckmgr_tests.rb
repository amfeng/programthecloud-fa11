require 'rubygems'
require 'bud'
require '../lckmgr'
require 'test/unit'

class Locker 
  include Bud
  include TwoPhaseLockMgr 

  state do
  end

  bloom do
  end

end

class TestLockMgr < Test::Unit::TestCase
  @lm = nil
  def setup
    @lm = Locker.new()
    @lm.run_bg
  end

  def tick(n = 3)
    n.times { @lm.sync_do }
  end

  def test_sharedlock_ok
    # Acquire a shared lock
    @lm.sync_do { @lm.request_lock <+ [ ["1", "A", :S] ] }
    tick

    @lm.locks.each do |l|
      if l.resource == "A"
        assert_equal(l.xid, "1")
        assert_equal(l.mode, :S)
      end
    end
    assert_equal(@lm.locks.length, 1)
  end

  def test_sharedlock_bad
    # Trying to acquire a :S lock on a resource that 
    # another Xact has a :X lock on
    @lm.sync_do { @lm.request_lock <+ [ ["2", "B", :X] ] }
    @lm.sync_do { @lm.request_lock <+ [ ["3", "B", :S] ] }
    tick

    @lm.locks.each do |l|
      if l.resource == "B"
        assert_equal(l.xid, "2")
        assert_equal(l.mode, :X)        
      end
    end
    assert_equal(@lm.locks.length, 1)
  end

  def test_exclusivelock_ok
    # Acquire an exclusive lock
    @lm.sync_do { @lm.request_lock <+ [ ["2", "B", :X] ] }
    tick

    @lm.locks.each do |l|
      if l.resource == "B"
        assert_equal(l.xid, "2")
        assert_equal(l.mode, :X)        
      end
    end
    assert_equal(@lm.locks.length, 1)
  end

  def test_exclusivelock_bad
    # Trying to acquire a :X lock on a resource when 
    # another Xact has a :S lock on it
    @lm.sync_do { @lm.request_lock <+ [ ["1", "A", :S] ] }
    tick
    @lm.sync_do { @lm.request_lock <+ [ ["3", "A", :X] ] }
    tick

    @lm.locks.each do |l|
      if l.resource == "A"
        assert_equal(l.xid, "1")
        assert_equal(l.mode, :S)        
      end
    end
    #puts @lm.locks.inspected
    assert_equal(@lm.locks.length, 1)
  end

  def test_sharedlocks_ok
    # Multiple Xacts can acquire a shared lock on a resource
    @lm.sync_do { @lm.request_lock <+ [ ["4", "C", :S] ] }
    tick
    @lm.sync_do { @lm.request_lock <+ [ ["5", "C", :S] ] }
    tick

    acquiredLocks = Array.new
    @lm.locks.each do |l|
      if l.resource == "C"
        assert_equal(l.mode, :S)        
        assert(["4", "5"].include?(l.xid))        
        acquiredLocks << l.xid
      end
    end
    acquiredLocks.sort!
    assert_equal(acquiredLocks[0], "4")
    assert_equal(acquiredLocks[1], "5")
    assert_equal(acquiredLocks.length, 2)
  end

  def test_downgrade
    # TODO: Clarify if a Xact has an exclusive lock, can it acquire
    # a shared lock. Will that result in a downgrade?
  
    # Trying to acquire a :S lock on a resource when 
    # the same Xact already has a :X lock on it
    @lm.sync_do { @lm.request_lock <+ [ ["6", "D", :X] ] }
    tick
    @lm.sync_do { @lm.request_lock <+ [ ["6", "D", :S] ] }
    tick

    @lm.locks.each do |l|
      if l.resource == "D"
        assert_equal(l.xid, "6")
        assert_equal(l.mode, :X)        
      end
    end
  end

  def test_upgrade
    # Lock upgrade
    @lm.sync_do { @lm.request_lock <+ [ ["7", "E", :S] ] }
    tick
    @lm.sync_do { @lm.request_lock <+ [ ["7", "E", :X] ] }
    tick(3)

    puts @lm.locks.inspected
    @lm.locks.each do |l|
      if l.resource == "E"
        assert_equal(l.xid, "7")
        assert_equal(l.mode, :X)        
      end
    end
    assert_equal(@lm.locks.length, 1)

    # Lock upgrade cannot happen if multiple Xacts have a :S lock
    @lm.sync_do { @lm.request_lock <+ [ ["8", "F", :S ] ]}
    tick
    @lm.sync_do { @lm.request_lock <+ [ ["9", "F", :S] ] }
    tick
    @lm.sync_do { @lm.request_lock <+ [ ["8", "F", :X] ] }
    tick

    acquiredLocks = Array.new
    @lm.locks.each do |l|
      if l.resource == "F"
        assert_equal(l.mode, :S)        
        assert(["8", "9"].include?(l.xid))        
        acquiredLocks << l.xid
      end
    end
    acquiredLocks.sort!
    assert_equal(acquiredLocks.at(0), "8")
    assert_equal(acquiredLocks.at(1), "9")
    assert_equal(acquiredLocks.length, 2)
  end

  def rest
    # Acquire a :X lock, end Xact, acquire a :S lock
    # @lm.sync_do { lm.request_lock <+ [ ["10", "G", "X"] ] }
    # 2.times {@lm.sync_do}
    # @lm.sync_do { lm.end_xact <+ [ ["10"] ] }
    # @lm.sync_do { lm.request_lock <+ [ ["11", "G", "S"] ] }
    # 2.times {@lm.sync_do}

    # @lm.locks.each do |l|
    #   if l.resource == "G"
    #     assert_equal(l.mode, "S")        
    #     assert_equal(l.xid, "11")        
    #   end
    # end

    # Acquire many :S locks, end one Xact 
    # Try to acquire a :X lock - fails
    # Try to acquire a :S lock - succeeds
    # @lm.sync_do { lm.request_lock <+ [ ["12", "H", "S"] ] }
    # @lm.sync_do { lm.request_lock <+ [ ["13", "H", "S"] ] }
    # 2.times {@lm.sync_do}
    # @lm.sync_do { lm.end_xact <+ [ ["12"] ] }
    # @lm.sync_do { lm.request_lock <+ [ ["14", "H", "X"] ] }
    # @lm.sync_do { lm.request_lock <+ [ ["15", "H", "S"] ] }
    # 2.times {@lm.sync_do}

    # acquiredLocks = Array.new
    # @lm.locks.each do |l|
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
    # @lm.sync_do { lm.request_lock <+ [ ["16", "I", "S"] ] }
    # @lm.sync_do { lm.request_lock <+ [ ["17", "I", "S"] ] }
    # 2.times {@lm.sync_do}
    # @lm.sync_do { lm.end_xact <+ [ ["16"] ] }
    # @lm.sync_do { lm.request_lock <+ [ ["17", "I", "X"] ] }

    # @lm.locks.each do |l|
    #   if l.resource == "I"
    #     assert_equal(l.xid, "17")
    #     assert_equal(l.mode, "X")        
    #   end
    # end

  end
end

