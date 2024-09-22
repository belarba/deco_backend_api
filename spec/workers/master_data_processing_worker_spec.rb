require 'rails_helper'

RSpec.describe MasterDataProcessingWorker, type: :worker do
  let(:file_path) { Rails.root.join('spec', 'fixtures', 'test_data.json').to_s }
  let(:job_id) { 'test_job_123' }

  before do
    # Stub minimal dependencies
    allow(Redis).to receive(:new).and_return(double('redis').as_null_object)
    allow(FileUtils).to receive(:mv)
    allow(Oj).to receive(:load_file).and_return([{ "id" => 1, "name" => "Test" }] * 15000)
    allow(Parallel).to receive(:each).and_yield([{ "id" => 1, "name" => "Test" }] * 10000, 0)
    allow(DataProcessingWorker).to receive(:perform_async)
  end

  it 'processes the file and enqueues DataProcessingWorker job' do
    expect(DataProcessingWorker).to receive(:perform_async).at_least(:once)
    described_class.new.perform(file_path, job_id)
  end

  it 'handles file not found error' do
    allow(FileUtils).to receive(:mv).and_raise(Errno::ENOENT.new("No such file or directory"))
    expect { described_class.new.perform(file_path, job_id) }.not_to raise_error
  end

  it 'handles JSON parse error' do
    allow(Oj).to receive(:load_file).and_raise(Oj::ParseError.new("Invalid JSON"))
    expect { described_class.new.perform(file_path, job_id) }.not_to raise_error
  end
end
