require 'rails_helper'

RSpec.describe MasterDataProcessingWorker, type: :worker do
  let(:worker) { MasterDataProcessingWorker.new }
  let(:redis) { instance_double(Redis) }
  let(:logger) { instance_double(Logger, info: nil, error: nil) }
  let(:job_id) { 'test_job_id' }
  let(:file_path) { 'test_file_path.json' }

  before do
    allow(Redis).to receive(:new).and_return(redis)
    allow(Logger).to receive(:new).and_return(logger)
    allow(redis).to receive(:close)
    allow(redis).to receive(:set)
    allow(DataProcessingWorker).to receive(:perform_async)
  end

  describe '#perform' do
    context 'with a valid JSON file' do
      let(:data_array) { [{ "key" => "value1" }, { "key" => "value2" }] }

      before do
        allow(Oj).to receive(:load_file).and_return(data_array)
        allow(Parallel).to receive(:each).and_yield(data_array, 0)
      end

      it 'enqueues DataProcessingWorker jobs' do
        expect(DataProcessingWorker).to receive(:perform_async).once

        worker.perform(file_path, job_id)
      end

      it 'sets Redis keys' do
        expect(redis).to receive(:set).with("data_processing:#{job_id}:total_chunks", 1)
        expect(redis).to receive(:set).with("data_processing:#{job_id}:processed_chunks", 0)
        expect(redis).to receive(:set).with("data_processing:#{job_id}:status", "processing")

        worker.perform(file_path, job_id)
      end
    end

    context 'with an invalid JSON file' do
      before do
        allow(Oj).to receive(:load_file).and_raise(Oj::ParseError.new("Invalid JSON"))
      end

      it 'logs an error' do
        expect(logger).to receive(:error).with("Error parsing JSON file: Invalid JSON")

        worker.perform(file_path, job_id)
      end
    end

    context 'when file is not found' do
      before do
        allow(Oj).to receive(:load_file).and_raise(Errno::ENOENT.new("File not found"))
      end

      it 'logs an error' do
        expect(logger).to receive(:error).with("File not found: No such file or directory - File not found")

        worker.perform(file_path, job_id)
      end
    end
  end
end
