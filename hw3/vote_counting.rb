require 'rubygems'
require 'bud'
require 'membership/membership'

module VoteCounting
  state do
    # Rows we've currently seen, and number of counts required
    table :num_required, [:reqtype] => [:num]
    table :rows, [:reqtype, :from, :reqid, :key, :version] => [:value]

    scratch :enough_acks, [:reqid, :reqtype]
    scratch :counts, [:reqid, :reqtype] => [:num]
    scratch :choose_max, [:reqtype, :from, :reqid, :key, :version] => [:value]
    scratch :chosen_max, [:reqid, :reqtype] => [:version]

    interface input, :incoming_gets, [:from, :reqid, :key, :version] => [:value]
    interface input, :incoming_puts, [:from, :reqid, :key]

    interface input, :acks_required, [:reqtype] => [:num]
    interface output, :result, [:reqtype, :reqid] => [:key, :value]
  end

  # Acknowledge that we want to start counting votes for some get request
  bloom :start do
    num_required <= acks_required
  end

  # Save the rows that come in, so we can count them
  bloom :save_rows do
    #stdio <~ incoming_puts.inspected
    rows <= incoming_gets {|g| [:read] + g }
    rows <= incoming_puts {|p| [:write] + p }
  end

  # For each counting request, count the rows, and if there are enough,
  # send the result back 
  bloom :count_rows do
    counts <= rows.group([:reqid, :reqtype], count()) 

    # Find the reqid's that have enough acks, we can go ahead and choose
    # the value with the higest timestamp from the results
    enough_acks <= (counts * num_required).pairs(:reqtype => :reqtype) { |c, r|
      [c.reqid, c.reqtype] if c.num >= r.num
    }
  end

  # Once we have enough acks for a read, find the one with the highest version
  # number
  bloom :process_read do
    # Aggregate all of the acks for the requests that have enough acks
    choose_max <= (enough_acks * rows).pairs(:reqid => :reqid) { |a, r|
      r if r.reqtype == :read 
    }

    # Find the max from each group of acks, and send them back
    chosen_max <= choose_max.group([:reqid, :reqtype], max(:version))

    # Join because we're missing the value row from the group by, and
    # return
    result <= (chosen_max * rows).pairs(:reqtype => :reqtype, :reqid => :reqid, :version => :version) { |c, r| [r.reqtype, r.reqid, r.key, r.value] }

    rows <- (chosen_max * rows).rights(:reqtype => :reqtype, :reqid => :reqid, :version => :version) 
  end

  # Once we have enough acks for a write, send the confirmation
  bloom :process_write do
    #stdio <~ enough_acks.inspected
    result <= (enough_acks * rows).pairs(:reqtype => :reqtype, :reqid => :reqid) { |a, r|
      [r.reqtype, r.reqid, r.key, :default_value] if r.reqtype == :write
    }
  end
end
