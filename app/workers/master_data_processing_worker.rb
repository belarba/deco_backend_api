class MasterDataProcessingWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'default', retry: 3

  CHUNK_SIZE = 10_000
  PARALLEL_PROCESSES = 4

  def perform(file_path, job_id)
    @logger = Logger.new($stdout)
    @redis = Redis.new
    @job_id = job_id

    process_file(file_path)
  rescue StandardError => e
    handle_error(e)
  ensure
    @redis.close
  end

  private

  def process_file(file_path)
    public_file_path = move_file_to_public(file_path)
    data_array = parse_json_file(public_file_path)
    process_data_chunks(data_array)
  end

  def move_file_to_public(file_path)
    file_path = file_path.to_s
    original_file_name = File.basename(file_path)
    new_file_name = "#{Time.now.strftime('%Y%m%d%H%M%S')}_#{original_file_name}"
    public_file_path = Rails.root.join('public', new_file_name).to_s

    FileUtils.mv(file_path, public_file_path)
    @logger.info("Moved file from #{file_path} to #{public_file_path}")

    update_redis('file_name', new_file_name)
    public_file_path
  end

  def parse_json_file(file_path)
    Oj.load_file(file_path, mode: :strict)
  rescue Oj::ParseError => e
    @logger.error("Error parsing JSON file: #{e.message}")
    raise
  end

  def process_data_chunks(data_array)
    total_chunks = (data_array.size / CHUNK_SIZE.to_f).ceil
    update_redis('total_chunks', total_chunks)
    update_redis('processed_chunks', 0)
    update_redis('status', 'processing')

    Parallel.each(data_array.each_slice(CHUNK_SIZE).with_index, in_processes: PARALLEL_PROCESSES) do |chunk, index|
      process_chunk(chunk, index)
    end

    @logger.info("Enqueued #{total_chunks} chunks for processing from #{@job_id}")
  end

  def process_chunk(chunk, index)
    sanitized_chunk = sanitize_data(chunk)
    DataProcessingWorker.perform_async(sanitized_chunk, index, @job_id)
  end

  def sanitize_data(data)
    case data
    when Hash then sanitize_hash(data)
    when Array then data.map { |item| sanitize_data(item) }
    else data
    end
  end

  def sanitize_hash(hash)
    hash.transform_values do |value|
      case value
      when String then sanitize_string(value)
      when Hash then sanitize_hash(value)
      when Array then sanitize_data(value)
      else value
      end
    end
  end

  def sanitize_string(string)
    string.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
  end

  def update_redis(key, value)
    @redis.set("data_processing:#{@job_id}:#{key}", value)
  end

  def handle_error(error)
    @logger.error("Error in MasterDataProcessingWorker: #{error.message}")
    @logger.error(error.backtrace.join("\n"))
  end
end
