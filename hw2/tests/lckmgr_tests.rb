require 'rubygems'
require 'bud'
require '../lckmgr'
require 'test/unit'

class Locker 
  include Bud
  include TwoPhaseLockMgr 

  state do
    table :timestamped, [:time, :src, :ident, :payload]
  end

  bloom do
    timestamped <= lock_status {|r| [budtime, r.xid, r.resource, r.status]}
  end

end

class TestLockMgr < Test::Unit::TestCase
  def test_lockmgr
    lm = Locker.new()

    lm.run_bg

  end
end

