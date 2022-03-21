require "timeout"
require_relative "connection_pool/version"

class ConnectionPool
  class Error < ::RuntimeError; end
  class PoolShuttingDownError < ::ConnectionPool::Error; end
  class TimeoutError < ::Timeout::Error; end
end

# Generic connection pool class for sharing a limited number of objects or network connections
# among many threads.  Note: pool elements are lazily created.
#
# Example usage with block (faster):
#
#    @pool = ConnectionPool.new { Redis.new }
#    @pool.with do |redis|
#      redis.lpop('my-list') if redis.llen('my-list') > 0
#    end
#
# Using optional timeout override (for that single invocation)
#
#    @pool.with(timeout: 2.0) do |redis|
#      redis.lpop('my-list') if redis.llen('my-list') > 0
#    end
#
# Example usage replacing an existing connection (slower):
#
#    $redis = ConnectionPool.wrap { Redis.new }
#
#    def do_work
#      $redis.lpop('my-list') if $redis.llen('my-list') > 0
#    end
#
# Accepts the following options:
# - :size - number of connections to pool, defaults to 5
# - :timeout - amount of time to wait for a connection if none currently available, defaults to 5 seconds
#
class ConnectionPool
  DEFAULTS = {size: 5, timeout: 5}

  def self.wrap(options, &block)
    Wrapper.new(options, &block)
  end

  # STEP_1
  # new
  # 
  # e.g.)  @pool = ConnectionPool.new { Redis.new } 
  def initialize(options = {}, &block)
    raise ArgumentError, "Connection pool requires a block" unless block

    # デフォルトを補完する
    options = DEFAULTS.merge(options)

    # オプションからfetchする
    @size = Integer(options.fetch(:size))
    @timeout = options.fetch(:timeout)

    # ConnectionPool::TimedStackを見る必要がある。-> STEP_2
    @available = TimedStack.new(@size, &block)
    @key = :"pool-#{@available.object_id}"
    @key_count = :"pool-#{@available.object_id}-count"
  end

  # STEP_3
  # with
  # このwithはどういったタイミングで呼ばれるのか？
  # ？ => ユースケースは後で
  # やってること
  # 
  #  checkout, checkin
  def with(options = {})
    Thread.handle_interrupt(Exception => :never) do
      # コネクションをチェックアウト
      conn = checkout(options)
      begin
        Thread.handle_interrupt(Exception => :immediate) do
          # ここはよくわからない
          yield conn
        end
      ensure
        # コネクションを返す。
        checkin
      end
    end
  end
  alias then with

  def checkout(options = {})
    # STEP_4
    # Therad.current => 現在のスレッドを返す
    # Thread#[]= => 各スレッドに固有のキーと値をセットできる
    # ここではkey, keycountというキーを使っているっぽい。
    # ここで
    # ThreadとConnectionを結びつける。
    # その結びつけ方として、KVを用いている。
    # 具体的には、
    # pool-object_id => Connectionの存在を管理
    # pool-object_id-count => Connectionの接続数を管理
    if ::Thread.current[@key] # => RedisConnectionとか
      ::Thread.current[@key_count] += 1
      ::Thread.current[@key]
    else
      ::Thread.current[@key_count] = 1
      # STEP_5
      # TimedStack#pop => STEP_6
      # 要するにコネクションを取得してくるやつ。できなかったらタイムアウトする。
      ::Thread.current[@key] = @available.pop(options[:timeout] || @timeout)
    end
  end

  # STEP_7
  # checkinする。
  # まず ::Thread.current[@key] => true すなわち接続があることを前提としています。
  # ::Thread.current[@key_count] == 1かどうかで場合分けしています。
  # まず１じゃない場合
  #
  #  ::Thread.current[@key_count] -= 1している。
  # つまり接続数を１つ減らす。？
  # では1の場合
  #
  # @available.push(::Thread.current[@key])
  # => STEP_8
  def checkin
    if ::Thread.current[@key]
      if ::Thread.current[@key_count] == 1
        # スレッド内で、接続数が1の時の解放
        # = このスレッドでは、もう接続を使わない
        # = queに接続を戻して、他のスレッドから使えるようにしてあげる。
        @available.push(::Thread.current[@key])
        ::Thread.current[@key] = nil
        ::Thread.current[@key_count] = nil
      else
        ::Thread.current[@key_count] -= 1
      end
    else
      raise ConnectionPool::Error, "no connections are checked out"
    end

    nil
  end

  ##
  # Shuts down the ConnectionPool by passing each connection to +block+ and
  # then removing it from the pool. Attempting to checkout a connection after
  # shutdown will raise +ConnectionPool::PoolShuttingDownError+.

  def shutdown(&block)
    @available.shutdown(&block)
  end

  ##
  # Reloads the ConnectionPool by passing each connection to +block+ and then
  # removing it the pool. Subsequent checkouts will create new connections as
  # needed.

  def reload(&block)
    @available.shutdown(reload: true, &block)
  end

  # Size of this connection pool
  attr_reader :size

  # Number of pool entries available for checkout at this instant.
  def available
    @available.length
  end
end

require_relative "connection_pool/timed_stack"
require_relative "connection_pool/wrapper"
