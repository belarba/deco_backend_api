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
    country = ActiveRecord::Base.connection.quote(params[:country])

    product_name_condition = params[:product_name].present? ? "product_name = #{product_name}" : "TRUE"
    country_condition = params[:country].present? ? "country = #{country}" : "TRUE"

    query = <<-SQL
      SELECT *
      FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY country DESC) as row_num
        FROM products
        WHERE #{product_name_condition} AND #{country_condition}
      ) as numbered_products
      WHERE row_num > #{offset} AND row_num <= #{offset + per_page}
    SQL

    count_query = <<-SQL
      SELECT COUNT(*)
      FROM products
      WHERE #{product_name_condition} AND #{country_condition}
    SQL

    products = ActiveRecord::Base.connection.execute(query)
    total_count = ActiveRecord::Base.connection.execute(count_query).first["count"].to_i

    countries = ActiveRecord::Base.connection.execute("SELECT DISTINCT country FROM products ORDER BY country").map { |row| row["country"] }

    render json: {
      products: products,
      countries: countries,
      meta: {
        current_page: page,
        total_pages: (total_count.to_f / per_page).ceil,
        total_count: total_count,
        per_page: per_page
      }
    }
  end

  def index_mongo
    per_page = (params[:per_page] || 20).to_i
    page = (params[:page] || 1).to_i

    query = ExternalRecord.all

    if params[:product_name].present?
      query = query.where(product_name: params[:product_name])
    end

    if params[:country].present?
      query = query.where(country: params[:country])
    end

    total_count = query.count

    products = query.order(country: :desc)
                    .skip((page - 1) * per_page)
                    .limit(per_page)
                    .to_a

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
