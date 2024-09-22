class MasterDataProcessingWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'default', retry: 3

  CHUNK_SIZE = 10_000 # Define o tamanho de cada chunk

  def perform(file_path, job_id)
    logger = Logger.new($stdout)
    redis = Redis.new

    begin
      # Converter file_path para string, caso seja um Pathname
      file_path = file_path.to_s

      # Mover o arquivo para a pasta public
      original_file_name = File.basename(file_path)
      timestamp = Time.now.strftime("%Y%m%d%H%M%S")
      new_file_name = "#{timestamp}_#{original_file_name}"
      public_file_path = Rails.root.join('public', new_file_name).to_s

      FileUtils.mv(file_path, public_file_path)
      logger.info("Moved file from #{file_path} to #{public_file_path}")

      # Usa Oj para parsing de JSON mais rápido
      data_array = Oj.load_file(public_file_path, mode: :strict)

      total_chunks = (data_array.size / CHUNK_SIZE.to_f).ceil

      redis.set("data_processing:#{job_id}:total_chunks", total_chunks)
      redis.set("data_processing:#{job_id}:processed_chunks", 0)
      redis.set("data_processing:#{job_id}:status", 'processing')
      redis.set("data_processing:#{job_id}:file_name", new_file_name)

      # Usa processamento paralelo para enfileirar os workers
      Parallel.each(data_array.each_slice(CHUNK_SIZE).with_index, in_processes: 4) do |chunk, index|
        sanitized_chunk = chunk.map { |item| sanitize_hash(item) }
        DataProcessingWorker.perform_async(sanitized_chunk, index, job_id)
      end

      logger.info("Enqueued #{total_chunks} chunks for processing from #{job_id}")
    rescue Oj::ParseError => e
      logger.error("Error parsing JSON file: #{e.message}")
    rescue Errno::ENOENT => e
      logger.error("File not found: #{e.message}")
    rescue StandardError => e
      logger.error("Unexpected error: #{e.message}")
      logger.error(e.backtrace.join("\n")) # Adiciona stack trace para debugging
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
