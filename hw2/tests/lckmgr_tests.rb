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

  def tick(n = 1)
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

  def test_locks_bad
    # Can't have both a shared and exclusive lock on a resource
    @lm.sync_do { @lm.request_lock <+ [ ["1", "A", :S], ["3", "A", :X] ] }
    tick(2)

    assert_equal(@lm.locks.length, 1)
    assert(@lm.queue.length == 1)
  end

  def test_locks_ok
    # Multiple Xacts can acquire a shared lock on a resource
    @lm.sync_do { @lm.request_lock <+ [ ["4", "C", :S],["5", "C", :S] ] }
    tick(2)

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
    @lm.sync_do { @lm.request_lock <+ [ ["7", "E", :X] ] }
    tick

    assert_equal(@lm.locks.length, 1)
    @lm.locks.each do |l|
      if l.resource == "E"
        assert_equal(l.xid, "7")
        assert_equal(l.mode, :X)        
      end
    end

    # Lock upgrade cannot happen if multiple Xacts have a :S lock
    @lm.sync_do { @lm.request_lock <+ [ ["8", "F", :S ], ["9", "F", :S] ] }
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
    assert_equal(acquiredLocks.length, 2)
    acquiredLocks.sort!
    assert_equal(acquiredLocks.at(0), "8")
    assert_equal(acquiredLocks.at(1), "9")
  end

  def test_releaselock_simple
    # Acquire a :X lock, end Xact, acquire a :S lock
    @lm.sync_do { @lm.request_lock <+ [ ["10", "G", :X] ] }
    @lm.sync_do { @lm.end_xact <+ [ ["10"] ] }
    @lm.sync_do { @lm.request_lock <+ [ ["11", "G", :S] ] }
    tick

    @lm.locks.each do |l|
      if l.resource == "G"
        assert_equal(l.mode, :S)        
        assert_equal(l.xid, "11")        
      end
    end
  end

  def test_releaselock 
    # Acquire many :S locks, end one Xact 
    # Try to acquire a :X lock - fails
    # Try to acquire a :S lock - succeeds
    @lm.sync_do { @lm.request_lock <+ [ ["12", "J", :S], ["13", "H", :S] ] }
    @lm.sync_do { @lm.end_xact <+ [ ["12"] ] }
    @lm.sync_do { @lm.request_lock <+ [ ["14", "J", :X],["15", "H", :S] ] }
    tick

    acquiredLocks = Array.new
    @lm.locks.each do |l|
      if l.resource == "H"
        assert_equal(l.mode, :S)        
        assert(["13", "15"].include?(l.xid))        
      elsif l.resource == "J"
        assert_equal(l.mode, :X)        
        assert(["14"].include?(l.xid))        
      end
      acquiredLocks << l.xid
    end

    acquiredLocks.sort!
    assert_equal(acquiredLocks.length, 3)
    assert_equal(acquiredLocks.at(0), "13")
    assert_equal(acquiredLocks.at(1), "14")
    assert_equal(acquiredLocks.at(2), "15")
  end

  def test_releaselock_upgrade
    # End Xact and perform a Lock upgrade
    @lm.sync_do { @lm.request_lock <+ [ ["16", "I", :S], ["17", "I", :S] ] }
    @lm.sync_do { @lm.end_xact <+ [ ["16"] ] }
    @lm.sync_do { @lm.request_lock <+ [ ["17", "I", :X] ] }
    tick

    @lm.locks.each do |l|
      if l.resource == "I"
        assert_equal(l.xid, "17")
        assert_equal(l.mode, :X)        
      end
    end
  end

  def test_suddenend
    # End a Xact when it still has pending locks 
    @lm.sync_do { @lm.request_lock <+ [ ["18", "I", :S], ["19", "I", :S], ["19", "J", :X] ] }
    tick
    @lm.sync_do { @lm.request_lock <+ [ ["19", "I", :X], ["20", "J", :S] ] }
    tick

    assert_equal(@lm.locks.length, 3)
    assert_equal(@lm.queue.length, 2)

    @lm.sync_do { @lm.end_xact <+ [ ["19"] ] }
    tick(2)

    @lm.locks.each do |l|
      if l.resource == "I"
        assert_equal(l.xid, "18")
        assert_equal(l.mode, :S)        
      end
    end
    assert_equal(@lm.locks.length, 2)
    assert_equal(@lm.queue.length, 0)
  end

  def test_blocking
    res = @lm.sync_callback(:request_lock, [[1, "foo", :S]], :lock_status)
    res = @lm.sync_callback(:request_lock, [[1, "bar", :S]], :lock_status)

    assert_equal([1, "bar", :OK], res.first)

    q = Queue.new
    @lm.register_callback(:lock_status) do |l|
      l.each do |row|
        if row.xid == 2
          q.push row
        end
      end
    end


    @lm.register_callback(:lock_status) do |l|
      l.each do |row|
        if row.xid == 3
          @lm.sync_do{ @lm.end_xact <+ [[3]]}
          @lm.sync_do
        end
      end
    end


    @lm.sync_do{ @lm.request_lock <+ [[2, "foo", :S]]}
    @lm.sync_do{ @lm.request_lock <+ [[3, "foo", :S]]}

    assert_equal(@lm.locks.length, 3)
    @lm.sync_do { @lm.end_xact <+ [[1]]}
    @lm.sync_do
    assert_equal(@lm.locks.length, 1)

    row = q.pop
    assert_equal([2, "foo", :OK], row)
    assert(true)
  end
end

