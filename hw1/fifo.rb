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

    # Buffer to keep requests received over pipe_chan until we can process
    # them in order
    table :intermediate, [:dst, :src, :ident] => [:payload]

    # Temporary "variable" for the current request chosen in order
    scratch :current, [:dst, :src] => [:ident, :payload]

    # Keep track of the counters for each src
    table :current_idents, [:src] => [:ident]
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
    # If the counter for something that just came in does not exist yet, 
    # create it
    #stdio <~ [current_idents {|c| ["#{c.src}, #{c.ident}"]}, ["---"]]

    # Find which src's are missing 
    temp :missing <= pipe_channel.notin(current_idents, :src => :src) 

    # If it doesn't exist, append a new row for the src
    # m[1] = src
    current_idents <+ missing {|m| [m[1], 0] } 
  end

  bloom :process do
    # On the receiver, add the packets in order to pipe_chan 
    #stdio <~ [["tick #{current_ident {|i| i.ident}} at #{budtime}"]]

    # Find the next packet to add to pipe_chan (according to the counter 
    # for this specific sender)
    current <= (intermediate * current_idents).lefts(:ident => :ident, :src => :src) 
    pipe_chan <+ current
    
    # Remove from intermediate
    intermediate <- current

    # Increment counter for this specific sender
    temp :current_ident <= (current * current_idents).pairs(:src => :src) { |u, c| [c.src, c.ident] }

    # c[0] = src, c[1] = ident
    current_idents <+- current_ident {|c| [c[0], c[1] + 1] }
  end

  bloom :done do
    pipe_sent <= pipe_chan 
  end
end
