class DataProcessingWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'default', retry: 3

  REMOVABLE_VALUES = ['BE', 'NK', 'FR', 'BE FE', 'BE NL', 'PT'].freeze
  BATCH_SIZE = 1000

  def perform(chunk, file_path, chunk_index)
    logger = Logger.new(STDOUT)
    redis = Redis.new

    postgresql_batch = []
    mongodb_batch = []

    begin
      chunk.each_with_index do |data, index|
        normalized_data = normalize_data(data)

        if data["availability"] && normalized_data[:price] > 0
          postgresql_batch << Product.new(normalized_data)
          mongodb_batch << ExternalRecord.new(normalized_data)
        end

        if (index + 1) % BATCH_SIZE == 0
          save_batches(postgresql_batch, mongodb_batch)
          postgresql_batch.clear
          mongodb_batch.clear
        end
      end

      save_batches(postgresql_batch, mongodb_batch) unless postgresql_batch.empty?

      # Incrementa o número de chunks processados
      processed_key = "data_processing:#{file_path}:processed_chunks"
      redis.incr(processed_key)

      # Verifica se todos os chunks foram processados
      if all_chunks_processed?(file_path, redis)
        File.delete(file_path) if File.exist?(file_path)
        logger.info("All chunks processed. Deleted file: #{file_path}")
      end

      logger.info("Processed chunk #{chunk_index} for #{file_path}")
    rescue StandardError => e
      logger.error("Error processing chunk #{chunk_index}: #{e.message}")
    ensure
      redis.close
    end
  end

  private

  def all_chunks_processed?(file_path, redis)
    total_chunks = redis.get("data_processing:#{file_path}:total_chunks").to_i
    processed_chunks = redis.get("data_processing:#{file_path}:processed_chunks").to_i
    processed_chunks >= total_chunks
  end

  def save_batches(postgresql_batch, mongodb_batch)
    ActiveRecord::Base.transaction do
      Product.import(postgresql_batch, validate: false)
    end
    ExternalRecord.collection.insert_many(mongodb_batch.map(&:attributes))
  rescue ActiveRecord::RecordInvalid => e
    logger.error("Error saving to PostgreSQL: #{e.message}")
  rescue Mongo::Error => e
    logger.error("Error saving to MongoDB: #{e.message}")
  end

  def normalize_data(data)
    shop_name = clean_invalid_chars(data["site"] || data["marketplaceseller"])
    country = clean_invalid_chars(data["country"])

    {
      country: country,
      brand: data["brand"],
      produtc_id: data["sku"].to_i,
      product_name: data["model"],
      product_category_id: data["categoryId"].to_i,
      shop_name: shop_name,
      price: data["price"].to_f,
      url: data["url"]
    }
  end

  def clean_invalid_chars(received_text)
    return nil if received_text.nil?

    REMOVABLE_VALUES.each do |invalid_value|
      received_text = received_text.gsub(/\b#{Regexp.escape(invalid_value)}\b/, '').strip
    end

    received_text.gsub(/\s+/, ' ')
  end
end
