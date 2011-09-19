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

  # Only allow one request per resource in at a time, for consistency
  bloom :gatekeeper do
    # Allowing reads (:S lock)

    # FIXME: Throws an error saying undefined method 'group' for #<Array>
    # temp :allow_readreq <= (request_lock.select { |r| r.mode == :S } << read_queue).group(:resource, choose(:xid))
    
    # FIXME: Delete this
    temp :allow_readreq <= (request_lock.select { |r| r.mode == :S })
                            
    request_read <+ allow_readreq
    read_queue <- allow_readreq

    read_queue <+ request_lock.notin(allow_readreq)

    # Allowing writes (:X lock)
    
    # FIXME: Throws an error saying undefined method 'group' for #<Array>
    # temp :allow_writereq <= (request_lock.select { |r| r.mode == :X } << write_queue).group(:resource, choose(:xid))
    
    # FIXME: Delete this
    temp :allow_writereq <= (request_lock.select { |r| r.mode == :X })

    request_write <+ allow_writereq
    read_queue <- allow_writereq

    read_queue <+ request_lock.notin(allow_readreq)
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
