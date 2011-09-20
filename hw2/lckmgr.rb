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
    scratch :request_read, request_lock.schema
    scratch :request_write, request_lock.schema
    
    table :read_queue, request_lock.schema 
    table :write_queue, request_lock.schema

    scratch :can_write, request_lock.schema
    scratch :can_upgrade, request_lock.schema

    table :locks, request_lock.schema

    # TODO: Better to keep redundant data or regenerate on the fly?
    scratch :write_locks, [:resource]
  end

  # Some locks have restrictions on the number of locks on a resource,
  # so doesn't make sense to process more than the allowed amount
  bloom :gatekeeper do
    stdio <~ [["tick #{budtime}"]]
    # Allowing reads (:S lock)

    # Add shared lock requests coming in to the read queue
    read_queue <+ request_lock.select { |r| r.mode == :S } 
    #stdio <~ read_queue.inspected

    # No restrictions on how many shared locks allowed on a resource
    # at once (unless there's an exclusive lock), we'll let them all
    # in for processing
    request_read <+ read_queue
    read_queue <- read_queue
    #stdio <~ request_read.inspected

    # Allowing writes (:X lock)
    
    # Add exclusive lock requests coming in to the read queue
    write_queue <+ request_lock.select { |r| r.mode == :X } 
    #stdio <~ write_queue.inspected
   
    # At most 1 exclusive lock per resource at a time, so we'll choose one per 
    # resource to process
    temp :allow_writereq <= write_queue.group([:resource, :mode], choose(:xid))
    #stdio <~ allow_writereq.inspected

    # Reorder columns because they got messed up in the grouping
    request_write <+ allow_writereq {|r| [r[2], r[0], r[1]] }
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
    temp :can_read <= request_read.notin(write_locks, :resource => :resource) 
    #stdio <~ write_locks.inspected
    #stdio <~ request_read.inspected
    #stdio <~ locks.inspected
    #stdio <~ can_read.inspected

    locks <+ can_read

    # TODO: If already have X lock, remove redundant S lock request from read_queue 
    read_queue <+ request_read.notin(can_read)
   end

  # For each write request that comes in, check if we can grant the lock:
  # if so, grant it, else, add it to the appropriate queue
  bloom :process_write do
    # Can grant write lock if currently no other locks on the resource held
    # by any other transaction
    can_write <= request_write.notin(locks, :resource => :resource) 
    #{ |r, l| true if r.xid != l.xid }
    can_upgrade <= (request_write * locks).lefts(:resource => :resource, :xid => :xid)
    #stdio <~ can_upgrade.inspected 
    #stdio <~ can_write.inspected
    #stdio <~ request_write.inspected
 
    locks <+ can_write
    # Replace any other locks for this resource/xid with :X lock
    locks <+- can_upgrade

    #stdio <~ locks.inspected

    # TODO: Remove from write_queue if not in can_upgrade as well
    write_queue <+ request_write.notin(can_write)
  end

  # At the end of a transaction, remove all of the locks that
  # transaction held
  bloom :remove_locks do
    locks <- (locks * end_xact).lefts(:xid => :xid)

    # TODO: Remove pending locks as well, in case a transaction ended abruptly
    # before getting all of the locks
  end
end
