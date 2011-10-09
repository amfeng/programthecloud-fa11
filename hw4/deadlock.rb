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
    table :link, [:from, :to] => []
    scratch :path, [:from, :to, :nodes] => []
    scratch :cycle, [:nodes] 

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
    cycle <= path { |p| [p.nodes] if p.from == p.to }
  end

  bloom :break_cycle do
  end

end

module DDLNode
  include LocalDeadlockDetector
end

module DDLMaster
  include DeadlockProtocol
end

