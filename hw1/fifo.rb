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
    #stdio <~ [current_idents {|c| ["#{c.src}, #{c.ident}"]}, ["---"]]

    # Check if the row exists
    temp :check_exists <= (pipe_channel * current_idents).pairs {|u, c| [c.src, c.ident] if u.src == c.src}

    # If not, append a new row for the src
    current_idents <+ pipe_channel {|x| [x.src, 0] unless check_exists.length > 0 } 

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
