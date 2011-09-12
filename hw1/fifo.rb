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
    channel :pipe_chan, [:@dst, :src, :ident] => [:payload]
    table :intermediate, [:dst, :src, :ident] => [:payload]
    table :current_ident, [] => [:ident]
    scratch :current, [] => [:dst, :src, :ident, :payload]
    periodic :clock, 1
  end

  bootstrap do
    current_ident <= [[0]]
  end

  bloom :snd do
    intermediate <= pipe_in
  end

  bloom :process do
    stdio <~ [["tick #{current_ident {|i| i.ident}} at #{budtime}"]]

    # Find the next request to send to pipe_chan (according to the counter)
    current <= (intermediate * current_ident).pairs { |p, i| p if p.ident == i.ident} 
    stdio <~ current
    pipe_chan <~ current
    
    # Remove from intermediate
    intermediate <- current

    # Update counter
    current_ident <- (current * current_ident).pairs {|u, c| [c.ident] }
    current_ident <+ (current * current_ident).pairs {|u, c| [c.ident + 1]}
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
