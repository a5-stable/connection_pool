##
# The TimedStack manages a pool of homogeneous connections (or any resource
# you wish to manage).  Connections are created lazily up to a given maximum
# number.

# Examples:
#
#    ts = TimedStack.new(1) { Object.new }
#
#    # fetch a connection
#    conn = ts.pop
#
#    # return a connection
#    ts.push conn
#
#    conn = ts.pop
#    ts.pop timeout: 5
#    #=> raises ConnectionPool::TimeoutError after 5 seconds

class ConnectionPool::TimedStack
  attr_reader :max

  ##
  # Creates a new pool with +size+ connections that are created from the given
  # +block+.

  # STEP_2
  # Create a new poolとは何か？
  #
  # &blockの例： Redis.new
  def initialize(size = 0, &block)
    @create_block = block
    @created = 0
    @que = []
    @max = size
    
    # https://docs.ruby-lang.org/ja/latest/class/Thread=3a=3aMutex.html
    # Therad::Mutexとはなんですか？
    # 1. Mutal Exclusion = 相互排他ロック
    # 共有データを並行アクセスから保護する
    # 各スレッドからMutexオブジェクトのロックを取り合う的な感じなのかな？
    # そのオブジェクトを作成している
    # push, pop, shutdown, empty?とかはConnectionPool#withで呼ばれる => STEP_3
    @mutex = Thread::Mutex.new
    @resource = Thread::ConditionVariable.new
    @shutdown_block = nil
  end

  ##
  # Returns +obj+ to the stack.  +options+ is ignored in TimedStack but may be
  # used by subclasses that extend TimedStack.
  #
  # obj => 接続そのもの
  def push(obj, options = {})
    # @mutex.synchronize
    # => ブロック内部でだけmutexをロックするやつ
    # 1. ロックしてstore_connectionする
    # @que.push obj
    # @queという配列[]にobjをpushする。
    # これがプールなのかな？
    #
    # 2. resource.broadcastする。
    @mutex.synchronize do
      if @shutdown_block
        @shutdown_block.call(obj)
      else
        store_connection obj, options
      end

      # ?????????????????
      @resource.broadcast
    end
  end
  alias << push

  ##
  # Retrieves a connection from the stack.  If a connection is available it is
  # immediately returned.  If no connection is available within the given
  # timeout a ConnectionPool::TimeoutError is raised.
  #
  # +:timeout+ is the only checked entry in +options+ and is preferred over
  # the +timeout+ argument (which will be removed in a future release).  Other
  # options may be used by subclasses that extend TimedStack.

  # STEP_6
  # コネクションを取得する
  #
  #
  def pop(timeout = 0.5, options = {})
    options, timeout = timeout, 0.5 if Hash === timeout
    timeout = options.fetch :timeout, timeout

    deadline = current_time + timeout

    # @mutex.synchronize
    # => ブロック内部でだけmutexをロックするやつ
    # connectionをcreateを
    # 取得できるまで かつ タイムアウトするまでloop試行する。
    @mutex.synchronize do
      loop do
        raise ConnectionPool::PoolShuttingDownError if @shutdown_block

        # 初回以外？
        return fetch_connection(options) if connection_stored?(options)

        # 初回
        # Redis.newとかが実行される
        connection = try_create(options)
        return connection if connection

        # タイムアウトする。
        # どういう時に取れないか
        # コネクションが解放されずにずっと unless @created == @maxの場合
        # あとはblock.callで異常に時間がかかっている場合とか。
        to_wait = deadline - current_time
        raise ConnectionPool::TimeoutError, "Waited #{timeout} sec" if to_wait <= 0

        # 何これ
        @resource.wait(@mutex, to_wait)
      end
    end
  end

  ##
  # Shuts down the TimedStack by passing each connection to +block+ and then
  # removing it from the pool. Attempting to checkout a connection after
  # shutdown will raise +ConnectionPool::PoolShuttingDownError+ unless
  # +:reload+ is +true+.

  def shutdown(reload: false, &block)
    raise ArgumentError, "shutdown must receive a block" unless block_given?

    @mutex.synchronize do
      @shutdown_block = block
      @resource.broadcast

      shutdown_connections
      @shutdown_block = nil if reload
    end
  end

  ##
  # Returns +true+ if there are no available connections.

  def empty?
    (@created - @que.length) >= @max
  end

  ##
  # The number of connections available on the stack.

  def length
    # 5 - 3 = 2 #=> 最大 - 作成数
    # 例えば@max - @created = 0でも、@queにある分だけ接続が使える。
    @max - @created + @que.length
  end

  private

  # なんだこれ
  # https://docs.ruby-lang.org/ja/latest/method/Process/m/clock_gettime.html
  def current_time
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must returns true if a connection is available on the stack.

  def connection_stored?(options = nil)
    !@que.empty?
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must return a connection from the stack.

  def fetch_connection(options = nil)
    @que.pop
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must shut down all connections on the stack.

  def shutdown_connections(options = nil)
    while connection_stored?(options)
      conn = fetch_connection(options)
      @shutdown_block.call(conn)
    end
    @created = 0
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must return +obj+ to the stack.

  def store_connection(obj, options = nil)
    @que.push obj
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must create a connection if and only if the total number of
  # connections allowed has not been met.

  def try_create(options = nil)
    # ここで渡したブロックが実行される！（Redis.newとか）
    unless @created == @max
      object = @create_block.call
      @created += 1
      object
    end
  end
end
