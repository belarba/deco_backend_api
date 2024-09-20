class DataProcessingWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'default', retry: 3

  REMOVABLE_VALUES = ['BE', 'NK', 'FR', 'BE FE', 'BE NL', 'PT'].freeze
  BATCH_SIZE = 1000

  def perform(file_path)
    # Lê o arquivo JSON
    data_array = JSON.parse(File.read(file_path))

    # Inicializa arrays para processamento em lote
    postgresql_batch = []
    mongodb_batch = []

    # Limpa os dados para garantir que estejam em UTF-8
    data_array.each do |data|
      clean_data(data)
    end

    # Normaliza os dados e armazena em lotes
    data_array.each_with_index do |data, index|
      normalized_data = normalize_data(data)

      # Regras de validação
      if data["availability"] && normalized_data[:price] > 0
        # Adiciona os dados ao lote
        postgresql_batch << Product.new(normalized_data)
        mongodb_batch << ExternalRecord.new(normalized_data)
      end

      # Insere em lote quando atingir o tamanho BATCH_SIZE
      if (index + 1) % BATCH_SIZE == 0
        save_batches(postgresql_batch, mongodb_batch)
        postgresql_batch.clear
        mongodb_batch.clear
      end
    end

    # Salva os últimos registros que não completaram o lote
    save_batches(postgresql_batch, mongodb_batch) unless postgresql_batch.empty?

    # Remove o arquivo após o processamento
    File.delete(file_path) if File.exist?(file_path)
  end

  private

  def save_batches(postgresql_batch, mongodb_batch)
    # Inserir em lote no PostgreSQL
    Product.import(postgresql_batch, validate: false)

    # Inserir em lote no MongoDB
    ExternalRecord.collection.insert_many(mongodb_batch.map(&:attributes))
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

  def clean_data(data)
    data.each do |key, value|
      if value.is_a?(String)
        data[key] = value.encode('UTF-8', invalid: :replace, undef: :replace, replace: '')
      elsif value.is_a?(Hash)
        clean_data(value)
      elsif value.is_a?(Array)
        value.each { |item| clean_data(item) if item.is_a?(Hash) }
      end
    end
  end

  # Função para limpar dados indesejados do nome da loja
  def clean_invalid_chars(received_text)
    return nil if received_text.nil?

    # Remover cada ocorrência dos valores inválidos
    REMOVABLE_VALUES.each do |invalid_value|
      received_text = received_text.gsub(/\b#{Regexp.escape(invalid_value)}\b/, '').strip
    end

    # Remove espaços duplicados que podem ter surgido após a limpeza
    received_text.gsub(/\s+/, ' ')
  end
end
