# frozen_string_literal: true

require 'rails_helper'

RSpec.describe MasterDataProcessingWorker, type: :worker do
  let(:file_path) { Rails.root.join('spec', 'fixtures', 'test_data.json').to_s }
  let(:job_id) { 'test_job_123' }
  let(:redis_double) { instance_double(Redis, set: nil, close: nil) }
  let(:logger_double) { instance_double(Logger, info: nil, error: nil) }

  before do
    allow(Redis).to receive(:new).and_return(redis_double)
    allow(Logger).to receive(:new).and_return(logger_double)
    allow(FileUtils).to receive(:mv)
    allow(Oj).to receive(:load_file).and_return([{ "id" => 1, "name" => "Test" }] * 15000)
    allow(Parallel).to receive(:each).and_yield([{ "id" => 1, "name" => "Test" }] * 10000, 0)
    allow(DataProcessingWorker).to receive(:perform_async)
    allow(Time).to receive_message_chain(:now, :strftime).and_return('20240922000000')
  end

  describe '#perform' do
    it 'processes the file and enqueues DataProcessingWorker jobs' do
      expect(DataProcessingWorker).to receive(:perform_async).at_least(:once)
      described_class.new.perform(file_path, job_id)
    end

    it 'moves the file to the public directory' do
      expect(FileUtils).to receive(:mv).with(file_path, Rails.root.join('public', '20240922000000_test_data.json').to_s)
      described_class.new.perform(file_path, job_id)
    end

    it 'logs the file movement' do
      expect(logger_double).to receive(:info).with(/Moved file from .* to .*/)
      described_class.new.perform(file_path, job_id)
    end

    context 'when file is not found' do
      before do
        allow(FileUtils).to receive(:mv).and_raise(Errno::ENOENT.new("No such file or directory"))
      end

      it 'logs the error and does not raise an exception' do
        expect(logger_double).to receive(:error).with(/Error in MasterDataProcessingWorker: No such file or directory/)
        expect { described_class.new.perform(file_path, job_id) }.not_to raise_error
      end
    end

    context 'when JSON parsing fails' do
      before do
        allow(Oj).to receive(:load_file).and_raise(Oj::ParseError.new("Invalid JSON"))
      end

      it 'logs the error and does not raise an exception' do
        expect(logger_double).to receive(:error).with(/Error in MasterDataProcessingWorker: Invalid JSON/)
        expect { described_class.new.perform(file_path, job_id) }.not_to raise_error
      end
    end
  end
end
