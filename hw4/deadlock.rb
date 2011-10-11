require '../lckmgr'

module DeadlockProtocol
  # A deadlock detection protocol outputs an array of mutually deadlocked
  # transaction ids, and a chosen victim.
  state do
    interface output, :deadlock, [:xidlist] => [:victim]
  end
end

module LocalDeadlockDetector
  include DeadlockProtocol
   
  state do
    scratch :link, [:from, :to] => []
    scratch :path, [:from, :to, :nodes] => []
    scratch :cycle, [:nodes] 

    scratch :victims, [:xid]

    interface input, :add_link, [:to, :from]
  end

  bloom :add_links do
    link <= add_link
  end

  bloom :cycle_detection do
    # Even if there are cycles, this will stop eventually because
    # cycles will insert duplicates
    # Ex. a -> b == a -> b, a -> b -> c -> d -> a == a -> b
    path <= link {|l| [l.from, l.to, l]}
    path <= (link * path).pairs(link.to => path.from) do |l,p|  
      [l.from, p.to, [l.from] | p.nodes] 
    end

    #stdio <~ path.inspected
    # A cycle means that there is some path betwen a node and itself
    # Ex. a -> a
    cycle <= path { |p| [p.nodes.sort] if p.from == p.to }
  end

  bloom :break_cycle do
    # Victim should always be the highest transaction id in the cycle
    deadlock <= cycle { |c| [c.nodes, c.first.max] }
  end
end

# Because this class includes the LocalDeadlockDetector module, once
# links are added to the deadlock detector, a deadlock will be reported
# via the deadlock interface output
module DLTwoPhase 
  include TwoPhaseLockMgr
  include LocalDeadlockDetector

  state do
    scratch :waits_for, [:waiter, :waitee]
  end

  bloom :waits_for do
    # For each item in the lock queue, figure out what it is waiting for
    # by which lock is the reason it can't get the resource
    waits_for <= (queue * locks).pairs(:resource => :resource) { |q, l|
      [q.xid, l.xid]
    }

    # Add the waits-for graph
    add_links <= waits_for
  end
end

module DDLNode
  include LocalDeadlockDetector
end

module DDLMaster
  include DeadlockProtocol
end
