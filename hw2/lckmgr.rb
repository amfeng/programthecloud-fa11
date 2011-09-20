require 'rubygems'
require 'bud'

module LockMgrProtocol
  state do
    interface input, :request_lock, [:xid, :resource] => [:mode]
    interface input, :end_xact, [:xid]
    interface output, :lock_status, [:xid, :resource] => [:status]
  end
end

module TwoPhaseLockMgr
  include LockMgrProtocol

  state do
    scratch :request_read, [:xid, :resource] => [:mode]
    scratch :request_write, [:xid, :resource] => [:mode]
    
    table :read_queue, [:xid, :resource] => [:mode]
    table :write_queue, [:xid, :resource] => [:mode]

    scratch :can_read, [:xid, :resource] => [:mode]
    scratch :can_write, [:xid, :resource] => [:mode]
    scratch :without_dups, [:xid, :resource] => [:mode]

    table :locks, [:xid, :resource] => [:mode]

    # TODO: Better to keep redundant data or regenerate on the fly?
    scratch :write_locks, [:resource]
  end

  bloom :debug do
    #stdio <~ [["tick #{budtime}"]]
    #stdio <~ read_queue.inspected
    #stdio <~ request_read.inspected
    #stdio <~ write_queue.inspected
    #stdio <~ allow_writereq.inspected

    #stdio <~ write_locks.inspected
    #stdio <~ request_read.inspected
    #stdio <~ locks.inspected
    #stdio <~ can_read.inspected

    #stdio <~ can_upgrade.inspected 
    #stdio <~ can_write.inspected
    #stdio <~ request_write.inspected
    #stdio <~ locks.inspected
  end

  # Some locks have restrictions on the number of locks on a resource,
  # so doesn't make sense to process more than the allowed amount
  bloom :gatekeeper do
    # Add shared lock requests coming in to the read queue
    read_queue <= request_lock.select { |r| r.mode == :S } 

    # No restrictions on how many shared locks allowed on a resource
    # at once (unless there's an exclusive lock), we'll let them all
    # in for processing
    request_read <= read_queue
    read_queue <- read_queue

    # Add exclusive lock requests coming in to the read queue
    write_queue <= request_lock.select { |r| r.mode == :X } 
   
    # At most 1 exclusive lock per resource at a time, so we'll choose one per 
    # resource to process
    temp :allow_writereq <= write_queue.group([:resource, :mode], choose(:xid))

    # Reorder columns because they got messed up in the grouping
    request_write <= allow_writereq {|r| [r[2], r[0], r[1]] }
    write_queue <- allow_writereq {|r| [r[2], r[0], r[1]] }
  end

  # For each read request that comes in, check if we can grant the lock:
  # if so, grant it, else, add it to the appropriate queue
  bloom :process_read do
    # Can grant read lock if currently no write locks on the resource held by 
    # any other transaction
    write_locks <= locks { |l| [l.resource] if l.mode == :X }

    # Don't downgrade lock; if already have X lock on a resource, ignore any S
    # lock requests
    can_read <= request_read.notin(write_locks, :resource => :resource) 

    locks <+ can_read
    lock_status <= can_read { |r| [r.xid, r.resource, :OK] }

    # TODO: If already have X lock, remove redundant S lock request from read_queue
    # and send OK lock_status

    # Reroute the read lock requests we couldn't grant back into the queue
    read_queue <+ request_read.notin(can_read)
   end

  # For each write request that comes in, check if we can grant the lock:
  # if so, grant it, else, add it to the appropriate queue
  bloom :process_write do
    # Can grant write lock if currently no other locks on the resource held
    # by any other transaction

    # Ignore locks that have the same resource and xid as one's we're requesting for,
    # we will be upgrading these locks
    without_dups <= locks.notin(request_write, :resource => :resource, :xid => :xid)
    can_write <= request_write.notin(without_dups, :resource => :resource) 

    # FIXME: Possible bug, write lock adds deferred until next timestamp, read lock
    # checking checks locks as of NOW
    locks <+- can_write
    lock_status <= can_write { |w| [w.xid, w.resource, :OK] }

    # Reroute the write lock requests we couldn't grant back into the queue
    write_queue <+ request_write.notin(can_write)
  end

  # At the end of a transaction, remove all of the locks that
  # transaction held
  bloom :remove_locks do
    locks <- (locks * end_xact).lefts(:xid => :xid)

    # Remove pending locks as well, in case a transaction ended abruptly
    # before getting all of the locks
    write_queue <- (write_queue * end_xact).lefts(:xid => :xid)
    read_queue <- (read_queue * end_xact).lefts(:xid => :xid)
  end
end
