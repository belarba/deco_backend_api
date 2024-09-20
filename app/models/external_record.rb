class ExternalRecord
  include Mongoid::Document
  include Mongoid::Timestamps

  field :country, type: String
  field :brand, type: String
  field :produtc_id, type: Integer
  field :product_name, type: String
  field :shop_name, type: String
  field :product_category_id, type: Integer
  field :price, type: Float
  field :url, type: String

end
