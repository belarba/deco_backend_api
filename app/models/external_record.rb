class ExternalRecord
  include Mongoid::Document
  include Mongoid::Timestamps

  field :country, type: String
  field :brand, type: String
  field :product_id, type: Integer
  field :product_name, type: String
  field :shop_name, type: String
  field :product_category_id, type: Integer
  field :price, type: Float
  field :url, type: String

   index({ country: 1 })
   index({ product_name: 1 })
end
