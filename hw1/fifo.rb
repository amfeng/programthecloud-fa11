require 'rubygems'
require 'bud'

module DeliveryProtocol
  state do
    interface input, :pipe_in, [:dst, :src, :ident] => [:payload]
    interface output, :pipe_sent, [:dst, :src, :ident] => [:payload]
  end
end

module FIFODelivery
  include DeliveryProtocol

  state do
    channel :pipe_channel, [:@dst, :src, :ident] => [:payload]
    scratch :pipe_chan, [:dst, :src, :ident] => [:payload]
    table :intermediate, [:dst, :src, :ident] => [:payload]
    table :current_idents, [:src] => [:ident]
    scratch :current_ident, [] => [:src, :ident]
    scratch :current, [] => [:dst, :src, :ident, :payload]
  end

  bootstrap do 
    current_idents <= [["localhost:54321", 0]]
  end

  bloom :snd do
    # On the sender, immediately send packets in pipe_in to the receiver
    pipe_channel <~ pipe_in
  end

  bloom :save do
    # On the receiver, add anything that came from the sender to an 
    # intermediate table for further processing
    intermediate <= pipe_channel
  end

  bloom :counter_init do
    # If the counter for something that just came in does not exist yet, create it

    #current_idents <+ pipe_channel {|x| [x.src, 0] unless (((current_idents * pipe_channel).pairs (:src => :src)).exists?) }
    temp :check_exists <= (pipe_channel * current_idents).pairs {|u, c| [c.src, c.ident] if u.src == c.src}
    #stdio <~ check_exists
    current_idents <+ pipe_channel {|x| [x.src, 0] unless check_exists.length > 0 } 
    #stdio <~ current_ident

    #current_idents <= (current { |x| [x.src, 0] } if (((current * current_idents).pairs {|u, c| [] if u.src == c.src }).length == 0))

    #current_ident <= (((current * current_idents).pairs {|u, c| [c.src, c.ident] if u.src == c.src }) or ([ current { |x| [x.src, 0] } ])) 
    current_idents <+- current_ident {|c| [c.src, c.ident + 1] }

    stdio <~ [current_idents {|c| ["#{c.src}, #{c.ident}"]}, ["---"]]
    #stdio <~ current_ident {|c| ["#{c.src}, #{c.ident}"]}
  end

  bloom :process do
    # On the receiver, add the packets in order to pipe_chan 
    #stdio <~ [["tick #{current_ident {|i| i.ident}} at #{budtime}"]]

    # Find the next packet to add to pipe_chan (according to the counter for this specific sender)
    current <= (intermediate * current_idents).pairs { |p, i| p if p.ident == i.ident and p.src == i.src} 
    pipe_chan <+ current
    
    # Remove from intermediate
    intermediate <- current

    # Increment counter for this specific sender
    current_ident <= (current * current_idents).pairs {|u, c| [c.src, c.ident] if u.src == c.src }

    current_idents <+- current_ident {|c| [c.src, c.ident + 1] }

  end

  bloom :done do
    pipe_sent <= pipe_chan 
  end
end

class SubFIFO
  include Bud
  include FIFODelivery

  state do
    table :timestamped, [:time, :ident, :payload]
  end

  bloom do
    timestamped <= pipe_chan {|c| [budtime, c.ident, c.payload]}
  end
end

=begin
fifo = SubFIFO.new
fifo.tick
fifo.pipe_in <+ [ ["localhost:00003", "localhost:54321", 3, "qux"] ]
fifo.tick
fifo.pipe_in <+ [ ["localhost:00001", "localhost:54321", 1, "bar"] ]
fifo.tick
fifo.pipe_in <+ [ ["localhost:00000", "localhost:54321", 0, "foo"] ]
fifo.tick
fifo.pipe_in <+ [ ["localhost:00002", "localhost:54321", 2, "baz"] ]
5.times {fifo.tick}


puts "==Intermediate=="
puts fifo.intermediate.length
puts fifo.intermediate.map {|t| "got #{t.ident}"}
puts "==Timestamped=="
puts fifo.timestamped.length
puts fifo.timestamped.map {|t| "sent #{t.ident} at #{t.time}"}
=end
