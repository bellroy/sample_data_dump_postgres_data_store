# frozen_string_literal: true

require 'dry/monads/all'
require 'duckface'
require 'rails'
require 'sample_data_dump/entities/table_configuration'
require 'sample_data_dump/interfaces/data_store_gateway'
require 'sample_data_dump_postgres_data_store/settings'

module SampleDataDumpPostgresDataStore
  class Gateway
    implements_interface SampleDataDump::Interfaces::DataStoreGateway

    def initialize(postgresql_adapter, settings)
      @squished_sql_runner = SquishedSqlRunner.new(postgresql_adapter)
      @settings = settings
    end

    def dump_to_local_file(table_configuration)
      dump_file = SampleDataDump::Helpers::DumpFile.new(table_configuration, @settings)
      uncompressed_file_path = dump_file.local_dump_file_path

      sql = extraction_sql(table_configuration)
      results = @squished_sql_runner.run(sql)
      export_results_to_sql(results, table_configuration, uncompressed_file_path)
      Dry::Monads::Success(uncompressed_file_path)
    end

    def load_dump_file(table_configuration)
      raise 'DO NOT LOAD OBFUSCATED DUMPS IN PRODUCTION!' if Rails.env.production?

      dump_file = SampleDataDump::Helpers::DumpFile.new(table_configuration, @settings)
      source_file_path = dump_file.local_dump_file_path
      unless File.exist?(source_file_path)
        return Dry::Monads::Failure("File #{source_file_path} does not exist for loading!")
      end

      sql = File.read(source_file_path)
      @squished_sql_runner.run(sql)
      Dry::Monads::Success(true)
    end

    def reset_sequence(table_configuration)
      table_name = table_configuration.qualified_table_name
      sql = "SELECT setval('#{table_name}_id_seq', coalesce((SELECT MAX(id) FROM #{table_name}),1))"
      @squished_sql_runner.run(sql)
      Dry::Monads::Success(true)
    end

    def valid?(table_configuration)
      TableConfigurationValidator.new(table_configuration, @squished_sql_runner).validation_result
    end

    def wipe_table(table_configuration)
      qualified_table_name = table_configuration.qualified_table_name
      @squished_sql_runner.run "DELETE FROM #{qualified_table_name} CASCADE"
      Dry::Monads::Success(true)
    end

    private

    attr_reader :settings
    delegate :compacted_dump_directory, :lorem_ipsum_function_schema, to: :settings

    class SquishedSqlRunner
      def initialize(postgresql_adapter)
        @postgresql_adapter = postgresql_adapter
      end

      def run(sql)
        @postgresql_adapter.execute sql.squish
      end
    end

    class TableConfigurationValidator
      include Dry::Monads::Do.for(:validation_result)

      def initialize(table_configuration, squished_sql_runner)
        @table_configuration = table_configuration
        @squished_sql_runner = squished_sql_runner
      end

      def validation_result
        yield schema_existence_result
        yield table_existence_result
        yield dump_where_condition_validity_result
        yield obfuscate_columns_validity_result
        Dry::Monads::Success(true)
      end

      private

      attr_reader :table_configuration
      delegate :dump_where,
               :obfuscate_columns,
               :qualified_table_name,
               :schema_name,
               :table_name,
               to: :table_configuration

      def schema_existence_result
        sql = <<~SQL
          SELECT EXISTS (
            SELECT *
            FROM pg_catalog.pg_namespace
            WHERE nspname = '#{schema_name}'
          );
        SQL
        results = @squished_sql_runner.run(sql)

        return Dry::Monads::Success(true) if results.first['exists']

        Dry::Monads::Failure("schema #{schema_name} does not exist")
      end

      def table_existence_result
        sql = <<~SQL
          SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables
            WHERE  table_schema = '#{schema_name}'
            AND    table_name = '#{table_name}'
          );
        SQL
        results = @squished_sql_runner.run(sql)

        return Dry::Monads::Success(true) if results.first['exists']

        Dry::Monads::Failure("#{qualified_table_name} does not exist")
      end

      def dump_where_condition_validity_result
        sql = <<~SQL
          SELECT * FROM #{qualified_table_name}
          WHERE #{dump_where}
          LIMIT 1
        SQL
        @squished_sql_runner.run(sql)
        Dry::Monads::Success(true)
      rescue ActiveRecord::StatementInvalid
        Dry::Monads::Failure("dump_where for #{qualified_table_name} invalid")
      end

      def obfuscate_columns_validity_result
        success_result = Dry::Monads::Success(true)
        return success_result if obfuscate_columns.empty?

        sql = "SELECT #{obfuscate_columns.join(', ')} FROM #{qualified_table_name} LIMIT 1"
        @squished_sql_runner.run(sql)
        success_result
      rescue ActiveRecord::StatementInvalid
        Dry::Monads::Failure("obfuscate_columns for #{qualified_table_name} invalid")
      end
    end

    def extraction_sql(table_configuration)
      columns = normal_columns(table_configuration) + lorem_ipsum_columns(table_configuration)
      <<~SQL
        SELECT #{columns.join(', ')}
        FROM #{table_configuration.qualified_table_name}
        WHERE #{table_configuration.dump_where}
        LIMIT 100000
      SQL
    end

    def normal_columns(table_configuration)
      columns = table_columns(table_configuration) - table_configuration.obfuscate_columns
      columns.map { |column| "\"#{column}\"" }
    end

    def table_columns(table_configuration)
      sql = <<~SQL
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '#{table_configuration.schema_name}'
        AND table_name = '#{table_configuration.table_name}'
      SQL
      @squished_sql_runner.run(sql).map { |row| row['column_name'] }
    end

    def lorem_ipsum_columns(table_configuration)
      table_configuration.obfuscate_columns.map do |column_name|
        "#{lorem_ipsum_function_schema}.lorem_ipsum(3) AS \"#{column_name}\""
      end
    end

    def export_results_to_sql(results, table_configuration, sql_file_path)
      row_count = results.ntuples
      if row_count.zero?
        `echo "SELECT 'No rows to load'" >> #{sql_file_path}`
      else
        File.open(sql_file_path, 'w+') do |file|
          table_name = table_configuration.qualified_table_name
          file.puts "DELETE FROM #{table_name};"
          insert_columns = table_columns(table_configuration)
          columns_in_quotes = insert_columns.map { |col| "\"#{col}\"" }.join(', ')
          file.puts "INSERT INTO #{table_name} (#{columns_in_quotes})"
          file.puts 'VALUES'
          results.each_with_index do |result, index|
            comma = index == row_count - 1 ? '' : ','
            values = columns_with_reserved_words_replaced(insert_columns).map { |col| result[col] }
            values = '(' + valid_postgres_values(values).join(', ') + ')' + comma
            file.puts(values)
          end
        end
      end
    end

    def columns_with_reserved_words_replaced(columns)
      columns.map do |column|
        if column == 'user'
          'current_user'
        else
          column
        end
      end
    end

    def valid_postgres_values(original_values)
      original_values.map do |value|
        if value.nil?
          'NULL'
        elsif value.is_a?(String) || value.is_a?(Date) || value.is_a?(Time)
          "'#{value.to_s.gsub("'", "''")}'"
        else
          value
        end
      end
    end
  end
end
