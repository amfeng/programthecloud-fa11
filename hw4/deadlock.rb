require '../lckmgr'

module DeadlockProtocol
  # A deadlock detection protocol outputs an array of mutually deadlocked
  # transaction ids, and a chosen victim.
  state do
    interface output, :deadlock, [:xidlist] => [:victim]
  end
end

module DDLNodeProtocol
  state do
    interface input, :set_coordinator, [:coordinator]
  end
end

module DDLNodeCommunicationProtocol
  state do
    channel :pipe_channel, [:@dst, :src] => [:from, :to]
  end
end

module LockMgrWaitsForGraph
  include TwoPhaseLockMgr

  state do
    scratch :waits_for, [:waiter, :waitee]
  end

  bloom :waits_for do
    # For each item in the lock queue, figure out what it is waiting for
    # by which lock is the reason it can't get the resource
    waits_for <= (queue * locks).pairs(:resource => :resource) { |q, l|
      [q.xid, l.xid]
    }
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
# links are added, a deadlock will be reported via the deadlock output 
module DLLockManager
  include LocalDeadlockDetector
  include LockMgrWaitsForGraph

  bloom :send_graph do
    # Add the waits-for graph
    add_links <= waits_for
  end
end

module DDLNode
  include DDLNodeProtocol
  include DDLNodeCommunicationProtocol

  state do
    table :coordinator, [] => [:coordinator]
  end

  bloom :set_coordinator do
    coordinator <= set_coordinator 
  end
  
  bloom :send_graph do
    pipe_channel <~ (coordinator * waits_for).pairs { |c, w|
      [c.coordinator, ip_port, w.from, w.to]
    }
  end
end

# Because this class includes the LocalDeadlockDetector module, once
# links are added, a deadlock will be reported via the deadlock output 
module DDLMaster
  include DDLNodeCommunicationProtocol
  include LocalDeadlockDetector

  bloom :apply_graph do
    # FIXME: If not all graphs come in at once, problem (this solution
    # only assuming 1 total timestep)
    add_link <= pipe_channel { |p| [p.from, p.to] }
  end
end

