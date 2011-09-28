require 'rubygems'
require 'bud'
require 'membership/membership'

module VoteCounting
  state do
    table :countsRequired, [:numRequired]
    table :rows, [:from, :reqid, :key, :version] => [:value]
    table :enoughAcks, [:reqid]

    scratch :counts, [:reqid] => [:number]
    scratch :chooseMax, [:from, :reqid, :key, :version] => [:value]
    scratch :chosenMax, [:reqid] => [:version]

    interface input, :incomingRows, [:from, :reqid, :key, :version] => [:value]
    interface input, :numberRequired, [:reqid] => [:requiredNumberOfElements]
    interface output, :result, [:reqid] => [:key, :value]
    interface output, :ackedReqids, [:reqid]
  end

  # Acknowledge that we want to start counting votes for some get request
  bloom :start do
    countsRequired <= numberRequired
  end

  # Save the rows that come in, so we can count them
  bloom :save_rows do
    rows <= incomingRows
  end

  # For each counting request, count the rows, and if there are enough,
  # send the result back 
  bloom :countRows do
    counts <= rows.group([:reqid], count()) 

    # Find the reqid's that have enough acks, we can go ahead and choose
    # the value with the higest timestamp from the results
    enoughAcks <= (counts * countsRequired).pairs { |c, r|
      [c.reqid] if c.number >= r.numRequired
    }

    # Aggregate all of the acks for the requests that have enough acks
    chooseMax <= (enoughAcks * rows).rights(:reqid => :reqid)

    # Find the max from each group of acks, and send them back
    chosenMax <= chooseMax.group([:reqid], max(:version))

    stdio <~ chosenMax.inspected
    # Join because we're missing the value row from the group by, and
    # return
    result <= (chosenMax * rows).pairs(:reqid => :reqid, :version => :version) { |c, r| [r.reqid, r.key, r.value] }
  end
end
