class DataProcessingWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'default', retry: 3

  def perform(file_path)
    # Lê o arquivo JSON
    data_array = JSON.parse(File.read(file_path))

    # Limpa os dados para garantir que estejam em UTF-8
    data_array.each do |data|
      clean_data(data)
    end

    # Normaliza os dados
    data_array.each do |data|
      normalized_data = normalize_data(data)

      # Salva no PostgreSQL
      product = Product.create!(normalized_data)

      # Salva no MongoDB
      ExternalRecord.create!(normalized_data)
    end
  end

  private

  def normalize_data(data)
    {
      country: data["country"],
      brand: data["brand"],
      produtc_id: data["sku"].to_i,
      product_name: data["model"],
      product_category_id: data["categoryId"].to_i,
      shop_name: data["site"] || data["marketplaceseller"],
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

end
