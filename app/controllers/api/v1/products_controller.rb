class Api::V1::ProductsController < ApplicationController

  skip_before_action :verify_authenticity_token

  def index
    products, total_count = fetch_products_from_db
    countries = fetch_countries

    render json: {
      products: products,
      countries: countries,
      meta: pagination_meta(total_count)
    }
  end

  def create
    if params[:file].present?
      job_id = process_uploaded_file
      render json: { status: 'Started the processing', job_id: job_id }, status: :ok
    else
      render json: { status: 'File not found' }, status: :not_found
    end
  end

  def index_mongo
    products, total_count = fetch_products_from_mongo

    render json: {
      products: products,
      meta: pagination_meta(total_count)
    }
  end

  private

  def fetch_products_from_db
    query = build_sql_query
    count_query = build_count_query

    products = ActiveRecord::Base.connection.execute(query)
    total_count = ActiveRecord::Base.connection.execute(count_query).first['count'].to_i

    [products, total_count]
  end

  def build_sql_query
    <<-SQL.squish
      SELECT *
      FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY country DESC) as row_num
        FROM products
        WHERE #{product_name_condition} AND #{country_condition}
      ) as numbered_products
      WHERE row_num > #{offset} AND row_num <= #{offset + per_page}
    SQL
  end

  def build_count_query
    <<-SQL.squish
      SELECT COUNT(*)
      FROM products
      WHERE #{product_name_condition} AND #{country_condition}
    SQL
  end

  def product_name_condition
    params[:product_name].present? ? "product_name = #{quote(params[:product_name])}" : 'TRUE'
  end

  def country_condition
    params[:country].present? ? "country = #{quote(params[:country])}" : 'TRUE'
  end

  def fetch_countries
    ActiveRecord::Base.connection
                      .execute('SELECT DISTINCT country FROM products ORDER BY country')
                      .pluck('country')
  end

  def process_uploaded_file
    file_path = Rails.root.join('tmp', params[:file].original_filename)
    job_id = SecureRandom.uuid
    File.binwrite(file_path, params[:file].read)

    MasterDataProcessingWorker.perform_async(file_path.to_s, job_id)
    job_id
  end

  def fetch_products_from_mongo
    query = build_mongo_query
    total_count = query.count

    products = query.order(country: :desc)
                    .skip(offset)
                    .limit(per_page)
                    .to_a

    [products, total_count]
  end

  def build_mongo_query
    query = ExternalRecord.all
    query = query.where(product_name: params[:product_name]) if params[:product_name].present?
    query = query.where(country: params[:country]) if params[:country].present?
    query
  end

  def pagination_meta(total_count)
    {
      current_page: page,
      total_pages: (total_count.to_f / per_page).ceil,
      total_count: total_count,
      per_page: per_page
    }
  end

  def per_page
    (params[:per_page] || 20).to_i
  end

  def page
    (params[:page] || 1).to_i
  end

  def offset
    (page - 1) * per_page
  end

  def quote(value)
    ActiveRecord::Base.connection.quote(value)
  end
end
