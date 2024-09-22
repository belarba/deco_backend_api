require 'rails_helper'

RSpec.describe DataProcessingWorker, type: :worker do
  let(:worker) { described_class.new }
  let(:redis) { instance_double(Redis) }
  let(:logger) { instance_double(Logger, info: nil, error: nil) }
  let(:job_id) { 'test_job_id' }
  let(:chunk_index) { 0 }

  before do
    allow(Redis).to receive(:new).and_return(redis)
    allow(Logger).to receive(:new).and_return(logger)
    allow(redis).to receive(:close)
    allow(redis).to receive(:incr)
    allow(redis).to receive(:get)
    allow(redis).to receive(:set)
    allow(Product).to receive(:insert_all)
    allow(ExternalRecord).to receive_message_chain(:collection, :insert_many)
  end

  describe '#perform' do
    let(:chunk) do
      [
        { 'availability' => true, 'price' => 100, 'ismarketplace' => false, 'site' => 'TestSite',
          'country' => 'US', 'brand' => 'TestBrand', 'sku' => '123', 'model' => 'TestModel',
          'categoryId' => '456', 'url' => 'http://test.com' }
      ]
    end

    it 'processes data and saves to databases' do
      expect(Product).to receive(:insert_all)
      expect(ExternalRecord.collection).to receive(:insert_many)

      worker.perform(chunk, chunk_index, job_id)
    end

    it 'increments processed chunks count' do
      expect(redis).to receive(:incr).with("data_processing:#{job_id}:processed_chunks")

      worker.perform(chunk, chunk_index, job_id)
    end

    context 'when all chunks are processed' do
      before do
        allow(redis).to receive(:get).with("data_processing:#{job_id}:total_chunks").and_return('1')
        allow(redis).to receive(:get).with("data_processing:#{job_id}:processed_chunks").and_return('1')
      end

      it 'sets status to completed' do
        expect(redis).to receive(:set).with("data_processing:#{job_id}:status", 'completed')
        expect(logger).to receive(:info).with("All chunks processed for job: #{job_id}.")

        worker.perform(chunk, chunk_index, job_id)
      end

      it 'does not delete the file' do
        expect(FileUtils).not_to receive(:rm_f)

        worker.perform(chunk, chunk_index, job_id)
      end
    end
  end
end
