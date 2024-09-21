class Api::V1::ProcessingStatusController < ApplicationController
  def show
    job_id = params[:job_id]
    redis = Redis.new

    status = redis.get("data_processing:#{job_id}:status")
    total_chunks = redis.get("data_processing:#{job_id}:total_chunks").to_i
    processed_chunks = redis.get("data_processing:#{job_id}:processed_chunks").to_i

    render json: {
      status:,
      total_chunks:,
      processed_chunks:,
      progress: total_chunks.positive? ? (processed_chunks.to_f / total_chunks * 100).round(2) : 0
    }
  ensure
    redis.close
  end
end
