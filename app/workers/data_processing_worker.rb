class DataProcessingWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'default', retry: 3

  REMOVABLE_VALUES = Set.new(['AL', 'AD', 'AM', 'AT', 'BY', 'BE', 'BA', 'BG',
    'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'GE', 'DE', 'GR', 'HU', 'IS',
    'IE', 'IT', 'KZ', 'XK', 'LV', 'LI', 'LT', 'LU', 'MT', 'MD', 'MC', 'ME',
    'NL', 'MK', 'NO', 'PL', 'PT', 'RO', 'RU', 'SM', 'RS', 'SK', 'SI', 'ES',
    'SE', 'CH', 'TR', 'UA', 'UK', 'VA', 'BR'])
  REMOVABLE_PATTERN = Regexp.union(REMOVABLE_VALUES.map { |v| /\b#{Regexp.escape(v)}\b/ })
  BATCH_SIZE = 1000

  def perform(chunk, file_path, chunk_index, job_id)
    logger = Logger.new(STDOUT)
    redis = Redis.new

    postgresql_batch = []
    mongodb_batch = []

    begin
      chunk.each_with_index do |data, index|
        normalized_data = normalize_data(data)

        if data["availability"] && normalized_data[:price] > 0
          postgresql_batch << normalized_data
          mongodb_batch << normalized_data
        end

        if (index + 1) % BATCH_SIZE == 0
          save_batches(postgresql_batch, mongodb_batch)
          postgresql_batch.clear
          mongodb_batch.clear
        end
      end

      save_batches(postgresql_batch, mongodb_batch) unless postgresql_batch.empty?

      processed_key = "data_processing:#{job_id}:processed_chunks"
      redis.incr(processed_key)

      if all_chunks_processed?(job_id, redis)
        File.delete(file_path) if File.exist?(file_path)
        redis.set("data_processing:#{job_id}:status", "completed")
        logger.info("All chunks processed. Deleted file: #{job_id}")
      end

      logger.info("Processed chunk #{chunk_index} for #{job_id}")
    rescue StandardError => e
      logger.error("Error processing chunk #{chunk_index}: #{e.message}")
      redis.set("data_processing:#{job_id}:status", "error")
    ensure
      redis.close
      GC.start # Libera memória não utilizada
    end
  end

  private

  def all_chunks_processed?(job_id, redis)
    total_chunks = redis.get("data_processing:#{job_id}:total_chunks").to_i
    processed_chunks = redis.get("data_processing:#{job_id}:processed_chunks").to_i
    processed_chunks >= total_chunks
  end

  def save_batches(postgresql_batch, mongodb_batch)
    # Inserção em massa para PostgreSQL
    Product.insert_all(postgresql_batch)

    # Inserção em massa para MongoDB
    ExternalRecord.collection.insert_many(mongodb_batch)
  rescue StandardError => e
    logger.error("Error saving batches: #{e.message}")
  end

  def normalize_data(data)
    shop_name = clean_invalid_chars(data["site"] || data["marketplaceseller"])
    country = clean_invalid_chars(data["country"])

    {
      country: country,
      brand: data["brand"].upcase,
      product_id: data["sku"].to_i,
      product_name: data["model"].upcase,
      product_category_id: data["categoryId"].to_i,
      shop_name: shop_name,
      price: data["price"].to_f,
      url: data["url"].downcase,
    }
  end

  def clean_invalid_chars(received_text)
    return nil if received_text.nil?

    received_text.upcase
                 .gsub(REMOVABLE_PATTERN, '')
                 .strip
                 .gsub(/\s+/, ' ')
  end
end
