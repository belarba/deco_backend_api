class DataProcessingWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'default', retry: 3

  REMOVABLE_VALUES = Set.new(%w[AL AD AM AT BY BE BA BG HR CY CZ DK EE FI FR GE DE GR HU IS
                                IE IT KZ XK LV LI LT LU MT MD MC ME NL MK NO PL PT RO RU SM RS
                                SK SI ES SE CH TR UA UK VA BR]).freeze
  REMOVABLE_PATTERN = Regexp.union(REMOVABLE_VALUES.map { |v| /\b#{Regexp.escape(v)}\b/ })
  BATCH_SIZE = 1_000

  def perform(chunk, chunk_index, job_id)
    @logger = Logger.new($stdout)
    @redis = Redis.new
    @job_id = job_id
    @chunk_index = chunk_index

    process_chunk(chunk)
  rescue StandardError => e
    handle_error(e)
  ensure
    cleanup
  end

  private

  def process_chunk(chunk)
    postgresql_batch = []
    mongodb_batch = []

    chunk.each_with_index do |data, index|
      process_data_item(data, postgresql_batch, mongodb_batch)
      save_and_clear_batches(postgresql_batch, mongodb_batch) if ((index + 1) % BATCH_SIZE).zero?
    end

    save_and_clear_batches(postgresql_batch, mongodb_batch) unless postgresql_batch.empty?
    update_progress
  end

  def process_data_item(data, postgresql_batch, mongodb_batch)
    normalized_data = normalize_data(data)
    if data['availability'] && normalized_data[:price].positive?
      postgresql_batch << normalized_data
      mongodb_batch << normalized_data
    end
  end

  def save_and_clear_batches(postgresql_batch, mongodb_batch)
    save_batches(postgresql_batch, mongodb_batch)
    postgresql_batch.clear
    mongodb_batch.clear
  end

  def save_batches(postgresql_batch, mongodb_batch)
    Product.insert_all(postgresql_batch)
    ExternalRecord.collection.insert_many(mongodb_batch)
  rescue StandardError => e
    @logger.error("Error saving batches: #{e.message}")
  end

  def update_progress
    @redis.incr("data_processing:#{@job_id}:processed_chunks")
    if all_chunks_processed?
      @redis.set("data_processing:#{@job_id}:status", 'completed')
      @logger.info("All chunks processed for job: #{@job_id}.")
    end
    @logger.info("Processed chunk #{@chunk_index} for #{@job_id}")
  end

  def all_chunks_processed?
    total_chunks = @redis.get("data_processing:#{@job_id}:total_chunks").to_i
    processed_chunks = @redis.get("data_processing:#{@job_id}:processed_chunks").to_i
    processed_chunks >= total_chunks
  end

  def normalize_data(data)
    {
      country: clean_invalid_chars(data['country']),
      brand: data['brand'].upcase,
      product_id: data['sku'].to_i,
      product_name: data['model'].upcase,
      product_category_id: data['categoryId'].to_i,
      shop_name: normalize_shop_name(data),
      price: data['price'].to_f,
      url: data['url'].downcase
    }
  end

  def normalize_shop_name(data)
    shop_name = data['ismarketplace'] ? data['marketplaceseller'] : data['site']
    clean_invalid_chars(shop_name)
  end

  def clean_invalid_chars(text)
    return nil if text.nil?

    text.upcase
        .gsub(REMOVABLE_PATTERN, '')
        .strip
        .gsub(/\s+/, ' ')
  end

  def handle_error(error)
    @logger.error("Error processing chunk #{@chunk_index}: #{error.message}")
    @redis.set("data_processing:#{@job_id}:status", 'error')
  end

  def cleanup
    @redis.close
    GC.start
  end
end
