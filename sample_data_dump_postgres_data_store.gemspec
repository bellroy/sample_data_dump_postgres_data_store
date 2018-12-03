# frozen_string_literal: true

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sample_data_dump_postgres_data_store/version'

Gem::Specification.new do |spec|
  spec.name          = 'sample_data_dump_postgres_data_store'
  spec.version       = SampleDataDumpPostgresDataStore::VERSION
  spec.authors       = ['Bellroy Dev Team']
  spec.email         = ['techsupport@bellroy.com']

  spec.summary       = 'Data storage for `sample_data_dump` gem - PostgreSQL'
  spec.description   = ''
  spec.homepage      = 'http://www.bellroy.com'

  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = 'https://rubygems.org'
  else
    raise 'RubyGems 2.0 or newer is required to protect against ' \
      'public gem pushes.'
  end

  spec.files = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_dependency 'activerecord'
  spec.add_dependency 'dry-matcher'
  spec.add_dependency 'dry-monads'
  spec.add_dependency 'dry-struct'
  spec.add_dependency 'dry-types'
  spec.add_dependency 'duckface-interfaces'
  spec.add_dependency 'pg'
  spec.add_dependency 'railties'
  spec.add_dependency 'sample_data_dump', '>= 0.0.2'

  spec.add_development_dependency 'bundler', '>= 1.13'
  spec.add_development_dependency 'rake', '>= 10.0'
  spec.add_development_dependency 'rspec', '>= 3.0'
end
