require 'rubygems'
require 'bud'
require 'membership/membership'

module TwoPCVoteCounting
  state do
    # Rows we've currently seen, and number of counts required
    table :req_table, [:reqid] => [:num]
    table :rows, [:phase, :from, :reqid] => [:payload]

    scratch :counts, [:reqid, :phase, :payload] => [:num]

    interface input, :phase_one_acks, [:from, :reqid] => [:payload]
    interface input, :phase_two_acks, [:from, :reqid] => [:payload]

    interface input, :begin_votes, [:reqid] => [:num]

    # TODO: Generalize into one output channel
    interface output, :phase_one_voting_result, [:reqid] => [:value]
    interface output, :phase_two_voting_result, [:reqid] => [:value]
  end

  # Acknowledge that we want to start counting votes for some get request
  bloom :start do
    req_table <= begin_votes
  end

  # Save the rows that come in, so we can count them
  bloom :save_rows do
    # Discard rows that we aren't currently counting
    rows <= (phase_one_acks * req_table).lefts(:reqid => :reqid) {|p| 
      [:phase_one] + p 
    }
    rows <= (phase_two_acks * req_table).lefts(:reqid => :reqid) {|p| 
      [:phase_two] + p 
    }
  end

  bloom :count do
    counts <= rows.group([:reqid, :phase, :payload], count()) 
  end

  bloom :phase_one do
    # Abort immediately if we get a NO
    phase_one_voting_result <=  (phase_one_acks * req_table).pairs(:ident => :reqid) { |a, r|
      [r.reqid, :phase_one, :abort] if a.payload == :no      
    }

    # Find the reqid's that have enough :yes acks
    phase_one_voting_result <= (counts * num_required) { |c, r|
      [c.reqid, :commit] if (c.num == r.num and 
                                        c.phase == :phase_one and 
                                        c.payload == :yes)
    }

    # TODO: Abort if we time out
  end

  # FIXME: Do we even need to count for Phase 2?
  bloom :phase_two do
    phase_two_voting_result <= (counts * num_required) { |c, r|
      [c.reqid, :committed] if (c.num == r.num and 
                                        c.phase == :phase_two and 
                                        c.payload == :committed)
    }

    # FIXME: What do we do if there's an abort?
    # If a reqid has a :no to commit or :aborted ack - we can stop
    phase_two_voting_result <= (counts * num_required) { |c, r| 
      [c.ident, :aborted] if c.payload == :aborted
    }
  end

  bloom :cleanup do
    # Clear finished rows out of the req_table
    req_table <- (phase_one_voting_result * req_table).rights(:reqid => :reqid) 
    req_table <- (phase_two_voting_result * req_table).rights(:reqid => :reqid) 

    # Clear finished rows out of the rows
    rows <- (phase_one_voting_result * rows).rights(:reqid => :reqid) 
    rows <- (phase_two_voting_result * rows).rights(:reqid => :reqid) 
  end
end
