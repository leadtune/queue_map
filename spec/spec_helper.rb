require 'rubygems'
require 'bundler'
Bundler.setup

require 'rspec'
require 'pathname'
require "background_process"

ROOT_PATH = Pathname(File.expand_path("../../", __FILE__))
LIB_PATH  = ROOT_PATH + "lib"
BIN_PATH  = ROOT_PATH + "bin"
SPEC_PATH = ROOT_PATH + "spec"

$: << LIB_PATH

require "queue_map"
require "queue_map/consumer"
require SPEC_PATH + "support/objects.rb"

QueueMap.consumer_base_path = SPEC_PATH + "support"

def File.truncate(filename)
  File.open(filename, "wb") {|f|}
end
