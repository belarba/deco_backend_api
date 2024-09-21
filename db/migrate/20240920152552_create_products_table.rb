class CreateProductsTable < ActiveRecord::Migration[7.1]
  def change
    create_table :products do |t|
      t.string :country
      t.string :brand
      t.integer :product_id
      t.string :product_name
      t.string :shop_name
      t.integer :product_category_id
      t.float :price
      t.string :url

      t.timestamps
    end
  end
end
