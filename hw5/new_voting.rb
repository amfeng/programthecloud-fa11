require 'rubygems'
require 'bud'
require 'membership/membership'

module TwoPCVoteCounting
  state do
    # Rows we've currently seen, and number of counts required
    table :req_table, [:reqid] => [:phase, :num, :timeout]
    table :rows, [:phase, :from, :reqid] => [:payload]
    table :timeout, [] => [:timeout]

    scratch :counts, [:reqid, :phase, :payload] => [:num]
    scratch :timed_out, [:reqid]
    scratch :acks, [:phase, :from, :reqid] => [:payload]

    interface input, :phase_one_acks, [:from, :reqid] => [:payload]
    interface input, :phase_two_acks, [:from, :reqid] => [:payload]

    interface input, :begin_votes, [:reqid] => [:num]

    # TODO: Generalize into one output channel
    interface output, :phase_one_voting_result, [:reqid] => [:value]
    interface output, :phase_two_voting_result, [:reqid] => [:value]

    periodic :timer, 1
  end

  # Acknowledge that we want to start counting votes for some get request
  bloom :start do
    req_table <= begin_votes 
  end

  # Save the rows that come in, so we can count them
  bloom :save_rows do
    acks <= phase_one_acks { |p| [:phase_one] + p }
    acks <= phase_two_acks { |p| [:phase_two] + p }

    # Discard rows that we aren't currently counting
    rows <= (acks * req_table).lefts(:reqid => :reqid, :phase => :phase)
  end

  bloom :count do
    counts <= rows.group([:reqid, :phase, :payload], count()) 
  end

  bloom :phase_one do
    # Abort immediately if we get a NO
    phase_one_voting_result <=  (phase_one_acks * req_table).pairs(:reqid=> :reqid) { |a, r|
      [r.reqid, :A] if a.payload == :no and r.phase == :phase_one     
    }

    # Find the reqid's that have enough :yes acks
    phase_one_voting_result <= (counts * num_required) { |c, r|
      [c.reqid, :C] if (c.num == r.num and 
                                        c.phase == :phase_one and 
                                        c.payload == :yes)
    }

    # Abort if we time out
    timed_out <= req_table { |r|
      [r.reqid] if r.phase == :phase one and r.timeout <= 0 
    }

    phase_one_voting_result <= timed_out { |t| [t.reqid, :A] }
    req_table <- (req_table * timed_out).lefts(:reqid => :reqid)
  end

  # Count the acks we get back after phase 2
  bloom :phase_two do
    phase_two_voting_result <= (counts * num_required) { |c, r|
      [c.reqid, :committed] if (c.num == r.num and 
                                        c.phase == :phase_two and 
                                        c.payload == :committed)
    }
  end

  bloom :timeout_tick do
    req_table <+- (req_table * timer).lefts { |r|
      [r.reqid, r.phase, r.num, r.timeout - 1]
    }
  end

  bloom :cleanup do
    # Clear finished rows out of the req_table
    req_table <- (phase_one_voting_result * req_table).rights(:reqid => :reqid) 
    req_table <- (phase_two_voting_result * req_table).rights(:reqid => :reqid) 

    # Clear out rows that aren't in the req_table anymore
    rows <- rows.notin(req_table, :reqid => :reqid, :phase => :phase)
  end
end
