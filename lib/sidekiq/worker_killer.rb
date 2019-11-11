require "get_process_mem"
require "sidekiq"
require "sidekiq/util"

module Sidekiq
  # Sidekiq server middleware. Kill worker when the RSS memory exceeds limit
  # after a given grace time.
  class WorkerKiller
    include Sidekiq::Util

    MUTEX = Mutex.new

    def initialize(options = {})
      @max_rss         = (options[:max_rss]         || 0)
      @grace_time      = (options[:grace_time]      || 15 * 60)
      @shutdown_wait   = (options[:shutdown_wait]   || 30)
      @kill_signal     = (options[:kill_signal]     || "SIGKILL")
    end

    def call(_worker, _job, _queue)
      yield
      # Skip if the max RSS is not exceeded
      return unless @max_rss > 0 && current_rss > @max_rss
      GC.start(full_mark: true, immediate_sweep: true)
      return unless @max_rss > 0 && current_rss > @max_rss
      # Launch the shutdown process
      warn "current RSS #{current_rss} of #{identity} exceeds " \
           "maximum RSS #{@max_rss}"
      request_shutdown
    end

    private

    def request_shutdown
      # In another thread to allow undelying job to finish
      Thread.new do
        # Only if another thread is not already
        # shutting down the Sidekiq process
        shutdown if MUTEX.try_lock
      end
    end

    def shutdown
      warn "sending quiet to #{identity}"
      sidekiq_process.quiet!

      warn "shutting down #{identity} in #{@grace_time} seconds"
      wait_job_finish_in_grace_time

      warn "stopping #{identity}"
      sidekiq_process.stop!

      warn "waiting #{@shutdown_wait} seconds before sending " \
            "#{@kill_signal} to #{identity}"
      sleep(@shutdown_wait)

      warn "sending #{@kill_signal} to #{identity}"
      ::Process.kill(@kill_signal, ::Process.pid)
    end

    def wait_job_finish_in_grace_time
      timeout = Time.now + @grace_time
      sleep(1) until timeout < Time.now || process_quiet_and_out_of_work?
    end

    def process_quiet_and_out_of_work?
      sidekiq_process.stopping? && sidekiq_process["busy"].zero?
    end

    def current_rss
      ::GetProcessMem.new.mb
    end

    def sidekiq_process
      Sidekiq::ProcessSet.new.find { |process|
        process["identity"] == identity
      } || raise("No sidekiq worker with identity #{identity} found")
    end

    def warn(msg)
      Sidekiq.logger.warn(msg)
    end
  end
end
