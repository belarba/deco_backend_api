class Api::V1::ProductsController < ApplicationController
  skip_before_action :verify_authenticity_token

  def create
    if params[:file].present?
      file_path = Rails.root.join("tmp", params[:file].original_filename)
      job_id = SecureRandom.uuid
      File.open(file_path, "wb") do |file|
        file.write(params[:file].read)
      end

      MasterDataProcessingWorker.perform_async(file_path.to_s, job_id)

      render json: { status: "Started the processing", job_id: job_id }, status: :ok
    else
      render json: { status: "File not found" }, status: :not_found
    end
  end

  def index
    per_page = (params[:per_page] || 20).to_i
    page = (params[:page] || 1).to_i
    offset = (page - 1) * per_page

    product_name = ActiveRecord::Base.connection.quote(params[:product_name])
    product_name_condition = params[:product_name].present? ? "product_name = #{product_name}" : "TRUE"

    query = <<-SQL
      SELECT *
      FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY country DESC) as row_num
        FROM products
        WHERE #{product_name_condition}
      ) as numbered_products
      WHERE row_num > #{offset} AND row_num <= #{offset + per_page}
    SQL

    count_query = <<-SQL
      SELECT COUNT(*)
      FROM products
      WHERE #{product_name_condition}
    SQL

    products = ActiveRecord::Base.connection.execute(query)

    total_count = ActiveRecord::Base.connection.execute(count_query).first["count"].to_i

    render json: {
      products: products,
      meta: {
        current_page: page,
        total_pages: (total_count.to_f / per_page).ceil,
        total_count: total_count,
        per_page: per_page
      }
    }
  end
end
