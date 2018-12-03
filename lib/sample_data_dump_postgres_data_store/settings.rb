# frozen_string_literal: true

require 'dry/struct'
require 'sample_data_dump/entities/settings'
require 'sample_data_dump/types'

module SampleDataDumpPostgresDataStore
  class Settings < SampleDataDump::Entities::Settings
    attribute :lorem_ipsum_function_schema,
              SampleDataDump::Types::Strict::String.default('public')
  end
end
