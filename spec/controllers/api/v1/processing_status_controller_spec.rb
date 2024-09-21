require 'rails_helper'

RSpec.describe Api::V1::ProcessingStatusController, type: :controller do
  describe 'GET #show' do
    let(:job_id) { 'test_job_id' }
    let(:redis_double) { instance_double(Redis) }

    before do
      allow(Redis).to receive(:new).and_return(redis_double)
      allow(redis_double).to receive(:close)
    end

    it 'returns the correct processing status' do
      expect(redis_double).to receive(:get).with("data_processing:#{job_id}:status").and_return('processing')
      expect(redis_double).to receive(:get).with("data_processing:#{job_id}:total_chunks").and_return('10')
      expect(redis_double).to receive(:get).with("data_processing:#{job_id}:processed_chunks").and_return('5')

      get :show, params: { job_id: job_id }

      expect(response).to have_http_status(:success)
      json_response = JSON.parse(response.body)
      expect(json_response['status']).to eq('processing')
      expect(json_response['total_chunks']).to eq(10)
      expect(json_response['processed_chunks']).to eq(5)
      expect(json_response['progress']).to eq(50.0)
    end

    it 'handles case when no chunks are processed' do
      expect(redis_double).to receive(:get).with("data_processing:#{job_id}:status").and_return('started')
      expect(redis_double).to receive(:get).with("data_processing:#{job_id}:total_chunks").and_return('0')
      expect(redis_double).to receive(:get).with("data_processing:#{job_id}:processed_chunks").and_return('0')

      get :show, params: { job_id: job_id }

      expect(response).to have_http_status(:success)
      json_response = JSON.parse(response.body)
      expect(json_response['progress']).to eq(0)
    end

    it 'ensures Redis connection is closed' do
      allow(redis_double).to receive(:get)
      expect(redis_double).to receive(:close)

      get :show, params: { job_id: job_id }
    end
  end
end
