class Api::V1::ProductsController < ApplicationController
  skip_before_action :verify_authenticity_token

  def create
    if params[:file].present?
      file_path = Rails.root.join("tmp", params[:file].original_filename)
      File.open(file_path, "wb") do |file|
        file.write(params[:file].read)
      end

      DataProcessingWorker.perform_async(file_path.to_s)

      render json: { status: "Started the processing" }, status: :ok
    else
      render json: { status: "File not found" }, status: :not_found
    end
  end
end
