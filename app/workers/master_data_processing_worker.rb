class MasterDataProcessingWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'default', retry: 3

  CHUNK_SIZE = 10000 # Define o tamanho de cada chunk

  def perform(file_path, job_id)
    logger = Logger.new(STDOUT)
    redis = Redis.new

    begin
      # Usa Oj para parsing de JSON mais rápido
      data_array = Oj.load_file(file_path, mode: :strict)

      total_chunks = (data_array.size / CHUNK_SIZE.to_f).ceil

      redis.set("data_processing:#{job_id}:total_chunks", total_chunks)
      redis.set("data_processing:#{job_id}:processed_chunks", 0)
      redis.set("data_processing:#{job_id}:status", "processing")

      # Usa processamento paralelo para enfileirar os workers
      Parallel.each(data_array.each_slice(CHUNK_SIZE).with_index, in_processes: 4) do |chunk, index|
        sanitized_chunk = chunk.map { |item| sanitize_hash(item) }
        DataProcessingWorker.perform_async(sanitized_chunk, file_path, index, job_id)
      end

      logger.info("Enqueued #{total_chunks} chunks for processing from #{job_id}")
    rescue Oj::ParseError => e
      logger.error("Error parsing JSON file: #{e.message}")
    rescue Errno::ENOENT => e
      logger.error("File not found: #{e.message}")
    rescue StandardError => e
      logger.error("Unexpected error: #{e.message}")
    ensure
      redis.close
    end
  end

  private

  def sanitize_hash(hash)
    hash.transform_values do |value|
      case value
      when String
        value.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
      when Hash
        sanitize_hash(value)
      when Array
        value.map { |v| v.is_a?(Hash) ? sanitize_hash(v) : v }
      else
        value
      end
    end
  end
end
