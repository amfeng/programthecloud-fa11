require 'rubygems'
require 'bud'
require 'membership/membership'

module TwoPCVoteCounting
  state do
    # Rows we've currently seen, and number of counts required
    table :num_required, [:num]
    table :rows, [:phase, :from, :ident] => [:payload]

    scratch :enough_acks, [:ident, :phase, :payload]
    scratch :counts, [:ident, :phase, :payload] => [:num]

    interface input, :phase_one_acks, [:from, :ident] => [:payload]
    interface input, :phase_two_acks, [:from, :ident] => [:payload]

    interface input, :acks_required, [:num]
    interface output, :phase_one_voting_result, [:ident, :phase] => [:value]
    interface output, :phase_two_voting_result, [:ident, :phase] => [:value]
  end

  # Acknowledge that we want to start counting votes for some get request
  bloom :start do
    num_required <= acks_required
  end

  # Save the rows that come in, so we can count them
  bloom :save_rows do
    rows <= phase_one_acks {|p| [:phase_one] + p }
    rows <= phase_two_acks {|p| [:phase_two] + p }
  end

  # For each counting request, count the rows
  bloom :count_rows do
    counts <= rows.group([:ident, :phase, :payload], count()) 

    # Find the reqid's that have enough :yes or :committed acks
    enough_acks <= (counts * num_required) { |c, r|
      [c.ident, c.phase, c.payload] if (c.num = r.num and 
                                        c.phase = :phase_one and 
                                        c.payload = :yes)
    }
    enough_acks <= (counts * num_required) { |c, r|
      [c.ident, c.phase, c.payload] if (c.num = r.num and 
                                        c.phase = :phase_two and 
                                        c.payload = :committed)
    }
    # If a reqid has a :no to commit or :aborted ack - we can stop
    enough_acks <= (counts * num_required) { |c, r| 
      [c.ident, c.phase, c.payload] if (c.payload = :no or 
                                        c.payload = :aborted)
    }
  end

  bloom :process_enough_acks do
    phase_one_voting_result <= (enough_acks * rows).pairs(:ident => :ident) { 
      |e,r| [e.ident, e.phase, e.payload] if e.phase = :phase_one
    }
    
    phase_two_voting_result <= (enough_acks * rows).pairs(:ident => :ident) { 
      |e,r| [e.ident, e.phase, e.payload] if e.phase = :phase_two
    }
    
    rows <- (enough_acks * rows).rights(:ident => :ident)
  end
end
