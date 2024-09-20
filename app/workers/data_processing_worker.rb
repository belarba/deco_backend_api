class DataProcessingWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'default', retry: 3

  def perform(file_path)
    # Lê o arquivo JSON
    data = JSON.parse(File.read(file_path))

    # Normaliza os dados
    normalized_data = normalize_data(data)

    # Salva no PostgreSQL
    record = Record.create!(name: normalized_data[:name], data: normalized_data[:data])

    # Salva no MongoDB
    ExternalRecord.create!(name: normalized_data[:name], data: normalized_data[:data])
  end

  private

  def normalize_data(data)
    {
      country: data["country"],
      brand: data["brand"],
      produtc_id: data["sku"],
      product_name: data["model"],
      shop_name: data["shop_name"],
      product_category_id: data["site"] || data["marketplaceseller"],
      price: data["price"],
      url: data["url"]
    }
  end
end
