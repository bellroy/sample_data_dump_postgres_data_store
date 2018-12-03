# frozen_string_literal: true

require 'duckface'
Dir["#{File.dirname(__FILE__)}/../../lib/**/*.rb"].each { |file| require file }

module Duckface
  describe ActsAsInterface do
    ObjectSpace.each_object(Duckface::ActsAsInterface).each do |interface|
      ObjectSpace.each_object(Class).each do |implementing_class|
        next unless implementing_class.included_modules.include?(interface)

        describe implementing_class do
          it_behaves_like 'it implements', interface
        end
      end
    end
  end
end
