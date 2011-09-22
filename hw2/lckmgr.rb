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
    # Persistent state for the current locks issued, and pending
    # lock requests
    table :locks, [:xid, :resource] => [:mode]
    table :queue, [:xid, :resource, :mode]

    # Temporary variables for current read requests in the request
    # pipeline
    scratch :request_read, [:xid, :resource] => [:mode]
    scratch :request_write, [:xid, :resource] => [:mode]

    # Temporary variables for accepted read/write lock requests and
    # temporary variables used to process those
    scratch :can_read, [:xid, :resource] => [:mode]
    scratch :can_write, [:xid, :resource] => [:mode]
    scratch :can_downgrade, [:xid, :resource] => [:mode]
    scratch :unique_locks, [:xid, :resource] => [:mode]
    
    # TODO: Better to keep redundant data or regenerate on the fly?
    scratch :write_locks, [:resource]

    # Temporary variable for ended transactions, so we know to remove
    # them from all of the deferred queues as well
    scratch :ended_xacts, [:xid]

    # Temporary variables for choosing which requests to process at a
    # timestep, to avoid "concurrent concerns"
    scratch :continuing_queue, [:xid, :resource, :mode]
    scratch :group_queue, [:resource] => [:xid]
    scratch :group_queue_single, [:xid, :resource, :mode]
    scratch :request_pipeline, [:xid, :resource] => [:mode]
  end

  # Some locks have restrictions on the number of locks on a resource,
  # so doesn't make sense to process more than the allowed amount
  bloom :gatekeeper do
    # Add lock requests coming in to the queue
    queue <= request_lock

    continuing_queue <= queue.notin(ended_xacts, :xid => :xid)
    group_queue <= continuing_queue.group([:resource], choose(:xid))
    group_queue_single <= (group_queue * continuing_queue).rights(:resource => :resource, :xid => :xid)
    request_pipeline <= group_queue_single.group([:xid, :resource], choose(:mode))
    queue <- request_pipeline
  end

  # For each read request that comes in, check if we can grant the lock:
  # if so, grant it, else, add it to the appropriate queue
  bloom :process_read do
    # Can grant read lock if currently no write locks on the resource held by 
    # any other transaction
    write_locks <= locks { |l| [l.resource] if l.mode == :X }
    request_read <= request_pipeline.select { |r| r.mode == :S }

    # Don't downgrade lock; if already have X lock on a resource, ignore any S
    # lock requests
    can_read <= request_read.notin(write_locks, :resource => :resource) 

    locks <+ can_read
    lock_status <= can_read { |r| [r.xid, r.resource, :OK] }

    # If already have X lock, remove redundant S lock request from read_queue
    # and send OK lock_status
    can_downgrade <= (request_read * locks).lefts(:resource => :resource, :xid => :xid)
    queue <- can_downgrade
    lock_status <= can_downgrade { |r| [r.xid, r.resource, :OK] }

    # Reroute the read lock requests we couldn't grant back into the queue
    queue <+ request_read.notin(can_read, :resource => :resource)
   end

  # For each write request that comes in, check if we can grant the lock:
  # if so, grant it, else, add it to the appropriate queue
  bloom :process_write do
    # Can grant write lock if currently no other locks on the resource held
    # by any other transaction
    request_write <= request_pipeline.select { |r| r.mode == :X }

    # Ignore locks that have the same resource and xid as one's we're requesting for,
    # we will be upgrading these locks
    unique_locks <= locks.notin(request_write, :resource => :resource, :xid => :xid)
    can_write <= request_write.notin(unique_locks, :resource => :resource) 

    locks <+- can_write
    lock_status <= can_write { |w| [w.xid, w.resource, :OK] }

    # Reroute the write lock requests we couldn't grant back into the queue
    queue <+ request_write.notin(can_write, :resource => :resource)
  end

  # At the end of a transaction, remove all of the locks that
  # transaction held
  bloom :remove_locks do
    locks <- (locks * end_xact).lefts(:xid => :xid)

    # Remove pending locks as well, in case a transaction ended abruptly
    # before getting all of the locks
    ended_xacts <=+ end_xact
    queue <- (queue * end_xact).lefts(:xid => :xid)
  end
end
