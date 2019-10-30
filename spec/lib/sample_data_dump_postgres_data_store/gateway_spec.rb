# frozen_string_literal: true

require 'active_record'
require 'active_record/connection_adapters/postgresql_adapter'
require 'sample_data_dump/gateways/local_file_system'
require 'sample_data_dump_postgres_data_store/gateway'

module SampleDataDumpPostgresDataStore
  describe Gateway do
    let(:gateway) do
      described_class.new(postgresql_adapter, settings)
    end

    let(:logger) { Logger.new(STDOUT) }
    let(:settings) do
      Settings.new(
        compacted_dump_directory: 'compacted_data_dump_test',
        config_file_path: File.dirname(__FILE__) + '/../../support/fixtures/sample_data_dump.yml',
        lorem_ipsum_function_schema: 'app_internals'
      )
    end

    let(:postgresql_adapter) do
      instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter)
    end
    let(:local_file_system_gateway) { SampleDataDump::Gateways::LocalFileSystem.new(settings) }

    let(:table_configuration) do
      SampleDataDump::Entities::TableConfiguration.new(
        schema_name: schema_name,
        table_name: table_name,
        dump_where: dump_where,
        obfuscate_columns: obfuscate_columns
      )
    end
    let(:schema_name) { 'my_schema_name' }
    let(:table_name) { 'my_table_name' }
    let(:dump_where) { 'column_name = 123' }
    let(:obfuscate_columns) { %w[contact_given_name] }

    let(:expect_column_retrieval) do
      sql = "SELECT column_name FROM information_schema.columns\n" \
            "WHERE table_schema = 'my_schema_name'\nAND table_name = 'my_table_name'\n"
      expect(postgresql_adapter).to receive(:execute).with(sql)
    end
    let(:expect_data_retrieval_with_obfuscated_column) do
      data_sql = "SELECT app_internals.lorem_ipsum(3) AS \"contact_given_name\"\n" \
                 "FROM my_schema_name.my_table_name\nWHERE column_name = 123\nLIMIT 100000\n"
      expect(postgresql_adapter).to receive(:execute).with(data_sql)
    end
    let(:expect_data_load_with_string) do
      insert_sql = "INSERT INTO my_schema_name.my_table_name (\"contact_given_name\")\n" \
                   "VALUES\n('lorem')\n"
      expect(postgresql_adapter).to receive(:execute).with(insert_sql)
    end

    after(:all) { FileUtils.rm_rf('compacted_data_dump_test') }

    describe '#dump_to_local_file' do
      subject(:dump_to_local_file) do
        gateway.dump_to_local_file(table_configuration)
      end

      context 'when string value' do
        before do
          expect_column_retrieval.twice.and_return([{ 'column_name' => 'contact_given_name' }])
          data_result = instance_double(PG::Result, ntuples: 1)
          expect(data_result)
            .to receive(:each_with_index).and_yield({ 'contact_given_name' => 'lorem' }, 0)
          expect_data_retrieval_with_obfuscated_column.and_return(data_result)
        end

        specify do
          local_file_system_gateway.clean_dump_directory
          expect(dump_to_local_file).to be_success
          expect(File.exist?(dump_to_local_file.value!)).to be true
          local_file_system_gateway.decompress_compressed_dump_file(table_configuration)

          expect_data_load_with_string
          gateway.load_dump_file(table_configuration)
        end
      end

      context 'when integer value' do
        let(:obfuscate_columns) { [] }

        before do
          expect_column_retrieval.twice.and_return([{ 'column_name' => 'contact_given_name' }])
          data_result = instance_double(PG::Result, ntuples: 1)
          expect(data_result)
            .to receive(:each_with_index).and_yield({ 'contact_given_name' => 1 }, 0)

          data_sql = "SELECT \"contact_given_name\"\n" \
                     "FROM my_schema_name.my_table_name\nWHERE column_name = 123\nLIMIT 100000\n"
          expect(postgresql_adapter).to receive(:execute).with(data_sql).and_return(data_result)
        end

        specify do
          local_file_system_gateway.clean_dump_directory
          expect(dump_to_local_file).to be_success
          expect(File.exist?(dump_to_local_file.value!)).to be true
          local_file_system_gateway.decompress_compressed_dump_file(table_configuration)

          insert_sql = "INSERT INTO my_schema_name.my_table_name (\"contact_given_name\")\n" \
                       "VALUES\n(1)\n"
          expect(postgresql_adapter).to receive(:execute).with(insert_sql)
          gateway.load_dump_file(table_configuration)
        end
      end

      context 'when reserved word column' do
        let(:obfuscate_columns) { [] }

        before do
          expect_column_retrieval.twice.and_return([{ 'column_name' => 'user' }])
          data_result = instance_double(PG::Result, ntuples: 1)
          expect(data_result)
            .to receive(:each_with_index).and_yield({ 'current_user' => 'bob' }, 0)

          data_sql = "SELECT \"user\"\n" \
                     "FROM my_schema_name.my_table_name\nWHERE column_name = 123\nLIMIT 100000\n"
          expect(postgresql_adapter).to receive(:execute).with(data_sql).and_return(data_result)
        end

        specify do
          local_file_system_gateway.clean_dump_directory
          expect(dump_to_local_file).to be_success
          expect(File.exist?(dump_to_local_file.value!)).to be true
          local_file_system_gateway.decompress_compressed_dump_file(table_configuration)

          insert_sql = "INSERT INTO my_schema_name.my_table_name (\"user\")\n" \
                       "VALUES\n('bob')\n"
          expect(postgresql_adapter).to receive(:execute).with(insert_sql)
          gateway.load_dump_file(table_configuration)
        end
      end

      context 'when NULL value' do
        let(:obfuscate_columns) { [] }

        before do
          expect_column_retrieval.twice.and_return([{ 'column_name' => 'contact_given_name' }])
          data_result = instance_double(PG::Result, ntuples: 1)
          expect(data_result)
            .to receive(:each_with_index).and_yield({ 'contact_given_name' => nil }, 0)

          data_sql = "SELECT \"contact_given_name\"\n" \
                     "FROM my_schema_name.my_table_name\nWHERE column_name = 123\nLIMIT 100000\n"
          expect(postgresql_adapter).to receive(:execute).with(data_sql).and_return(data_result)
        end

        specify do
          local_file_system_gateway.clean_dump_directory
          expect(dump_to_local_file).to be_success
          expect(File.exist?(dump_to_local_file.value!)).to be true
          local_file_system_gateway.decompress_compressed_dump_file(table_configuration)

          insert_sql = "INSERT INTO my_schema_name.my_table_name (\"contact_given_name\")\n" \
                       "VALUES\n(NULL)\n"
          expect(postgresql_adapter).to receive(:execute).with(insert_sql)
          gateway.load_dump_file(table_configuration)
        end
      end

      context do
        before do
          expect_column_retrieval.and_return([{ 'column_name' => 'contact_given_name' }])
          data_result = instance_double(PG::Result, ntuples: 0)
          expect_data_retrieval_with_obfuscated_column.and_return(data_result)
        end

        specify do
          local_file_system_gateway.clean_dump_directory
          expect(dump_to_local_file).to be_success
          expect(File.exist?(dump_to_local_file.value!)).to be true
          local_file_system_gateway.decompress_compressed_dump_file(table_configuration)

          expect(postgresql_adapter).to receive(:execute).with("SELECT 'No rows to load'\n")
          gateway.load_dump_file(table_configuration)
        end
      end
    end

    describe '#load_dump_file' do
      subject(:load_dump_file) do
        gateway.load_dump_file(table_configuration)
      end

      before do
        dump_file = SampleDataDump::Helpers::DumpFile.new(table_configuration, settings)
        FileUtils.rm_rf(dump_file.local_compressed_dump_file_path)
        FileUtils.rm_rf(dump_file.local_dump_file_path)
      end

      it { is_expected.to be_failure }

      context do
        before do
          expect_column_retrieval.twice.and_return([{ 'column_name' => 'contact_given_name' }])

          data_result = instance_double(PG::Result, ntuples: 1)
          expect(data_result)
            .to receive(:each_with_index).and_yield({ 'contact_given_name' => 'lorem' }, 0)
          expect_data_retrieval_with_obfuscated_column.and_return(data_result)

          expect_data_load_with_string

          gateway.dump_to_local_file(table_configuration)
          local_file_system_gateway.decompress_compressed_dump_file(table_configuration)
        end

        it { is_expected.to be_success }
      end
    end

    describe '#valid?' do
      subject(:valid?) { gateway.valid?(table_configuration) }

      let(:schema_exists) { false }

      before do
        sql = "SELECT EXISTS (\n  " \
              "SELECT *\n  FROM pg_catalog.pg_namespace\n  WHERE nspname = 'my_schema_name'\n);\n"
        expect(postgresql_adapter)
          .to receive(:execute).with(sql).and_return([{ 'exists' => schema_exists }])
      end

      it { is_expected.to be_failure }

      context 'when schema exists' do
        let(:schema_exists) { true }
        let(:table_exists) { false }

        before do
          sql = "SELECT EXISTS (\n  " \
                "SELECT 1\n  FROM   information_schema.tables\n  " \
                "WHERE  table_schema = 'my_schema_name'\n  " \
                "AND    table_name = 'my_table_name'\n);\n"
          expect(postgresql_adapter)
            .to receive(:execute).with(sql).and_return([{ 'exists' => table_exists }])
        end

        it { is_expected.to be_failure }

        context 'when table exists' do
          let(:table_exists) { true }
          let(:dump_where_condition) do
            "SELECT * FROM my_schema_name.my_table_name\nWHERE column_name = 123\nLIMIT 1\n"
          end

          context 'when dump_where condition valid' do
            before { expect(postgresql_adapter).to receive(:execute).with(dump_where_condition) }

            context do
              before do
                expect(postgresql_adapter)
                  .to receive(:execute)
                  .with('SELECT contact_given_name FROM my_schema_name.my_table_name LIMIT 1')
              end

              it { is_expected.to be_success }
            end

            context do
              before do
                expect(postgresql_adapter)
                  .to receive(:execute)
                  .with('SELECT contact_given_name FROM my_schema_name.my_table_name LIMIT 1')
                  .and_raise(ActiveRecord::StatementInvalid, 'error!')
              end

              it { is_expected.to be_failure }
            end
          end

          context 'when dump_where condition valid' do
            before do
              expect(postgresql_adapter)
                .to receive(:execute).with(dump_where_condition)
                .and_raise(ActiveRecord::StatementInvalid, 'error!')
            end

            it { is_expected.to be_failure }
          end
        end
      end
    end

    describe '#wipe_table' do
      subject(:wipe_table) { gateway.wipe_table(table_configuration) }

      before do
        expect(postgresql_adapter)
          .to receive(:execute).with('DELETE FROM my_schema_name.my_table_name CASCADE')
      end

      it { is_expected.to be_success }
    end

    describe '#reset_sequence' do
      subject(:reset_sequence) { gateway.reset_sequence(table_configuration) }

      context 'when sequence exists' do
        before do
          expect(postgresql_adapter)
            .to receive(:execute)
            .with("SELECT PG_GET_SERIAL_SEQUENCE('my_schema_name.my_table_name', 'id') AS name")
            .and_return([{ 'name' => sequence_name }])
        end

        context 'when is named' do
          let(:sequence_name) { 'wild_sequence' }

          before do
            expect(postgresql_adapter)
              .to receive(:execute)
              .with("SELECT setval('#{sequence_name}', " \
                    'coalesce((SELECT MAX(id) FROM my_schema_name.my_table_name),1))')
          end

          it { is_expected.to be_success }
        end

        context 'when is unnamed' do
          let(:sequence_name) { nil }

          it { is_expected.to be_success }
        end
      end

      context 'when sequence does not exist' do
        before do
          expect(postgresql_adapter)
            .to receive(:execute)
            .with("SELECT PG_GET_SERIAL_SEQUENCE('my_schema_name.my_table_name', 'id') AS name")
            .and_raise(ActiveRecord::StatementInvalid)
        end

        it { is_expected.to be_success }
      end
    end
  end
end
