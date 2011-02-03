require 'redis'

module BgExecutor
  # Redis backend
  class Redis
    def initialize
      redis
    rescue
      raise BgExecutor::ConnectionError, $!.message
    end

    # increments the value for +key+ by 1 and returns the new value
    def increment(key)
      redis.incr _key(key)
    end

    # decrements the value for +key+ by 1 and returns the new value
    def decrement(key)
      redis.decr _key(key)
    end

    # tests whether +key+ exists or not
    def exists?(key)
      redis.exists _key(key)
    end

    # retrieve data from redis by +key+
    def get(key)
      u redis.get(_key(key))
    end
    alias [] get

    # set +value+ for +key+ and optionally expiration time
    def set(key, value, expiry = 0)
      redis.set _key(key), s(value)
      redis.expire _key(key), expiry if expiry > 0
      value
    end

    # set +value+ for +key+ only when it's missing in cache
    def set_if_not_exists(key, value, expiry)
      set key, value, expiry unless exists? key
      value
    end

    # shortcut for set without expiration
    def []=(key, value)
      return delete(key) if value.nil?

      set(key, value)
    end

    # remove +key+ from cache
    def delete(key)
      redis.del _key(key)
    end
    alias unset delete

    # set expiration time for +key+
    def expire(key, expiry)
      redis.expire _key(key), expiry
    end

    # return first element of list and remove it
    def shift(key)
      u(redis.lpop _key(key))
    end

    # push new element at the beginning of the list
    def unshift(key, value)
      redis.lpush _key(key), s(value)
    end

    # push new element at the end of the list
    def push(key, value)
      redis.rpush _key(key), s(value)
    end

    # return last element of list and remove it
    def pop(key)
      u(redis.rpop _key(key))
    end

    # return LIST as ARRAY
    def list(key)
      return nil unless list?(key)

      result = []
      (redis.llen _key(key)).times do |idx|
        result << list_item(key, idx)
      end

      result
    end

    def list_item(key, idx)
      u(redis.lindex _key(key), idx)
    end

    # return list length of list
    def list_length(key)
      redis.llen(_key(key))
    end

    # is given +key+ is list
    def list?(key)
      redis.type(_key(key)) == "list"
    end

    # is given +key+ is string
    def string?(key)
      redis.type(_key(key)) == "string"
    end

    # execute +block+ with pessimistic locking
    def synchronize(mutex_id, &block)
      mutex_key = "mutex:#{mutex_id}"

      timeout(3) do
        loop do
          break unless exists?(mutex_key)
          sleep 0.05
        end
      end if exists?(mutex_key)

      set(mutex_key, 1, 6)
      result = yield
      delete(mutex_key)

      result
    end

    private
    def redis
      @redis ||= ::Redis.new(::BgExecutor::Configuration[:redis])
    end

    def serialize(data)
      Marshal.dump data
    end
    alias_method :s, :serialize

    def unserialize(data)
      Marshal.load data
    rescue
      nil
    end
    alias_method :u, :unserialize

    def _key(key)
      @key_suffix ||= ::BgExecutor::Configuration[:redis][:namespace]
      "#{@key_suffix}:#{key}"
    end
  end
end