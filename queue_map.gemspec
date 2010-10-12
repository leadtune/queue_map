# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'queue_map/version'

Gem::Specification.new do |s|
  s.name        = "queue_map"
  s.version     = QueueMap::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Tim Harper"]
  s.email       = ["tim@leadtune.com"]
  s.homepage    = "http://github.com/leadtune/queue_map"
  s.summary     = "Map a task across a pool of consumers, and recieve the result when the consumers are done"
  s.description = "Queue Map"

  s.required_rubygems_version = ">= 1.3.6"

  s.add_dependency "bunny", "0.5.3"
  s.add_development_dependency "rspec", "2.0.0.rc"
  s.add_development_dependency "background_process"
  s.add_development_dependency "ruby-debug"

  s.files        = Dir.glob("{bin,lib}/**/*") + %w(LICENSE README.md ROADMAP.md CHANGELOG.md)
  s.executables  = ['bin/queue_map_consumer']
  s.require_path = 'lib'
end
