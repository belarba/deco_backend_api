# Deco Backend API

This is an API developed to read a JSON file, save the data in 2 databases - PostgreSQL and MongoDB - and present and browse the saved data.

The User Interface can be found [here](https://github.com/belarba/deco_frontend_ui)

## Technologies and Versions Used

- Ruby 3.3.3
- Ruby on Rails 7.1.3
- PostgreSQL
- MongoDB
- Redis
- Sidekiq

## Installation

1. Clone the repository:

`git clone https://github.com/belarba/deco_backend_api.git`

2. Navigate to the project directory:

`cd deco_backend_api`

3. Install dependencies:

`bundle install`

## Database Setup

### PostgreSQL

1. Create the PostgreSQL database:

`rails db:create`

2. Run migrations:

`rails db:migrate`

### MongoDB

1. Ensure MongoDB is running on your system
2. Configure MOngoDB connection in `config/mongoid.yml` if necessary

## Redis Setup

1. Ensure Redis is running on your system

2. Configure Redis connection in `config/redis.yml` if necessary

## Running the Application

1. Start the Rails server:

`rails server`

2. In a separate terminal, start Sidekiq:

`bundle exec sidekiq`

The application should now be running at
`http://localhost:3000`

## API Endpoints

The API is versioned and namespaced. All endpoints are under `/api/v1/`

If you are using the [UI](https://github.com/belarba/deco_backend_api), ensure the add the full path of your application there `http://localhost:3000/api/v1` to have the correct usability

- Create a product
  - POST `/api/v1/products`

- List products (PostgreSQL)
  - GET `/api/v1/products`

- List products (MongoDB)
  - GET `api/v1/products_mongo`

- Check processing status
  - GET `api/v1/processing_status/:job_id`

## Sidekiq Web Interface

This project uses Sidekiq for background job processing. We have the web interface mounted at  `/sidekiq`. You can use to monitor and manage background jobs

## Running Tests

This project uses RSpec for testing. To run the test suite:

`bundle exec rspec`

## My approach

I started developing the application in the simplest way possible, without an initial focus on performance. This approach allowed me to quickly establish a functional base, but also revealed significant areas for optimization:

- In the first version of the import process, it took about 22 minutes to import approximately 240,000 records.
- By the end of development, the same process was optimized to complete in just 1 minute in my development environment.

### Development Process

My strategy evolved organically during the development process:

1. I focused on solving one problem at a time.

2. Improvements and optimizations emerged naturally, rather than being planned from the start.

3. This iterative approach allowed me to adapt the solution as I gained more insights into the project.

### Use of AI and Optimization Tools

I used artificial intelligence as a discovery tool to find better approaches and performance optimization possibilities:

1. Through AI, I discovered the Oj library for more efficient JSON parsing.

2. I implemented parallel processing using the Parallel gem, also based on AI suggestions. I knew I wanted to add it, bit I wasn't aware about how to do it.

### Challenges and Learnings

The development was not without obstacles:

- I faced significant difficulties in implementing process monitoring. Creating the processing status controller was particularly challenging and time-consuming. Initially, I had problems saving and retrieving information from Redis, which led me to try several different approaches. After a few attempts, I managed to find an ideal solution for managing the processing state.

- The testing phase proved to be particularly time-consuming and challenging. I had to experiment with various approaches to establish even basic functional tests. This project highlighted the difference between modifying existing tests (which I'm more accustomed to) and creating new ones from scratch, in my previous experiences, I typically worked with codebases that already had established test suites.

### Areas for Improvement

- *Environment Configuration*: Implement a `.env` file to manage all used URLs (PostgreSQL, MongoDB, and Redis) and credentials. This would enhance security and make the application more configurable.
  
- *Test Coverage*: Improve test scenarios to cover more edge cases. Expand testing to check additional edge scenarios not currently covered.

- *Error Handling*: Enhance the error handling mechanisms. I realized I focused too much on the happy path, and there are some simple error scenarios that could benefit from better error handling.

- *JSON File Import*: Create a list of records with UTF-8 problems, to review it and consider ways to address them without just ignoring these records.

- *Records read and pagination*: I don't think my approach is the best possible. I have the feeling that it doesn't work very well with large datasets and needs more study.

### Number of records imported

After filtering the specified rules and ignoring UTF-8 errors that I encountered while reading the JSON file, I was able to import 2_343_790 records
  
