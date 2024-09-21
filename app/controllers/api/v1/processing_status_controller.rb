class Api::V1::ProcessingStatusController < ApplicationController
  def show
    file_path = params[:file_path]
    redis = Redis.new

    status = redis.get("data_processing:#{file_path}:status")
    total_chunks = redis.get("data_processing:#{file_path}:total_chunks").to_i
    processed_chunks = redis.get("data_processing:#{file_path}:processed_chunks").to_i

    render json: {
      status: status,
      total_chunks: total_chunks,
      processed_chunks: processed_chunks,
      progress: total_chunks > 0 ? (processed_chunks.to_f / total_chunks * 100).round(2) : 0
    }
  ensure
    redis.close
  end
end
