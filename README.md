# Sample Data Dump - Postgres Data Store

This gem provides:
  - A PostgreSQL data storage gateway for the `sample_data_dump` gem

## Usage

See the [sample_data_dump README](https://github.com/tricycle/sample_data_dump) for more
instructions.

```
settings = SampleDataDumpPostgresDataStore::Settings.new(your_settings)
SampleDataDumpPostgresDataStore::Gateway.new(settings)
```

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'sample_data_dump_postgres_data_store', git: 'git@github.com:tricycle/sample_data_dump_postgres_data_store.git'
```

And then execute:

    $ bundle install
