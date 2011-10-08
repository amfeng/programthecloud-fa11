module DeadlockProtocol
  # a deadlock detection protocol outputs an array of mutually deadlocked
  # transaction ids, and a chosen victim.
  state do
    interface output, :deadlock, [:xidlist] => [:victim]
  end
end

module LocalDeadlockDetector
  include DeadlockProtocol
end

module DDLNode
  include LocalDeadlockDetector
end

module DDLMaster
  include DeadlockProtocol
end

