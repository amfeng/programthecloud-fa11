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

    table :locks, [:xid, :resource] => [:mode]

    # FIXME: Better to keep redundant data or regenerate on the fly?
    scratch :write_locks, [:resource]
  end

  # Some locks have restrictions on the number of locks on a resource,
  # so doesn't make sense to process more than the allowed amount
  bloom :gatekeeper do
    # Allowing reads (:S lock)

    # Add shared lock requests coming in to the read queue
    read_queue <+ request_lock.select { |r| r.mode == "S" } 

    # No restrictions on how many shared locks allowed on a resource
    # at once (unless there's an exclusive lock), we'll let them all
    # in for processing
    request_read <+ read_queue
    read_queue <- read_queue

    # Allowing writes (:X lock)
    
    # Add exclusive lock requests coming in to the read queue
    write_queue <+ request_lock.select { |r| r.mode == "X" } 
   
    # At most 1 exclusive lock per resource at a time, so we'll choose one per 
    # resource to process
    temp :allow_writereq <= write_queue.group([:resource], choose(:xid))

    request_write <+ allow_writereq
    write_queue <- allow_writereq

    write_queue <+ request_lock.notin(allow_writereq)
  end

  # For each read request that comes in, check if we can grant the lock:
  # if so, grant it, else, add it to the appropriate queue
  bloom :process_read do
    # Can grant read lock if currently no write locks on the resource
    write_locks <= locks { |l| l if l.mode == :X }
    temp :can_read <= request_read.notin(write_locks, :resource => :resource)

    locks <+ can_read
    read_queue <+ request_read.notin(can_read)
   end

  # For each write request that comes in, check if we can grant the lock:
  # if so, grant it, else, add it to the appropriate queue
  bloom :process_write do
    # Can grant write lock if currently no other locks on the resource
    temp :can_write <= request_write.notin(locks, :resource => :resource)

    locks <+ can_write
    write_queue <+ request_write.notin(can_write)
  end

  # At the end of a transaction, remove all of the locks that
  # transaction held
  bloom :remove_locks do
    locks <- (locks * end_xact).lefts(:xid => :xid)
  end
end
