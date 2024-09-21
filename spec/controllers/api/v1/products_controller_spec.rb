require 'rails_helper'

RSpec.describe Api::V1::ProductsController, type: :controller do
  describe 'POST #create' do
    context 'when file is not present' do
      it 'returns a not found status' do
        post :create

        expect(response).to have_http_status(:not_found)
        json_response = JSON.parse(response.body)
        expect(json_response['status']).to eq('File not found')
      end
    end
  end

  describe 'GET #index' do
    let(:mock_products) { [{ 'id' => 1, 'name' => 'Product 1', 'country' => 'USA' }] }
    let(:mock_count) { [{ 'count' => 100 }] }

    before do
      allow(ActiveRecord::Base.connection).to receive(:quote).and_return("'quoted_string'")
      allow(ActiveRecord::Base.connection).to receive(:execute).and_return(mock_products, mock_count)
    end

    it 'returns paginated products' do
      get :index, params: { per_page: 20, page: 1 }

      expect(response).to have_http_status(:success)
      json_response = JSON.parse(response.body)
      expect(json_response['products']).to eq(mock_products)
      expect(json_response['meta']).to include(
        'current_page' => 1,
        'total_pages' => 5,
        'total_count' => 100,
        'per_page' => 20
      )
    end

    it 'uses default pagination when not specified' do
      get :index

      expect(response).to have_http_status(:success)
      json_response = JSON.parse(response.body)
      expect(json_response['meta']['per_page']).to eq(20)
      expect(json_response['meta']['current_page']).to eq(1)
    end

    it 'filters products by name when provided' do
      expect(ActiveRecord::Base.connection).to receive(:quote).with('Test Product').and_return("'Test Product'")
      expect(ActiveRecord::Base.connection).to receive(:execute).twice.and_return(mock_products, mock_count)

      get :index, params: { product_name: 'Test Product' }

      expect(response).to have_http_status(:success)
    end

    it 'constructs the correct SQL query' do
      expected_query = %r{
        SELECT\s+\*\s+
        FROM\s+\(\s+
          SELECT\s+\*,\s+ROW_NUMBER\(\)\s+OVER\s+\(ORDER\s+BY\s+country\s+DESC\)\s+as\s+row_num\s+
          FROM\s+products\s+
          WHERE\s+TRUE\s+
        \)\s+as\s+numbered_products\s+
        WHERE\s+row_num\s+>\s+0\s+AND\s+row_num\s+<=\s+20
      }x

      expect(ActiveRecord::Base.connection).to receive(:execute).with(a_string_matching(expected_query)).and_return(mock_products)
      expect(ActiveRecord::Base.connection).to receive(:execute).with(a_string_including("SELECT COUNT(*)")).and_return(mock_count)

      get :index
    end
  end
end
