require 'fileutils'
require 'pathname'
require 'bundler/setup'
require 'sidekiq'
require 'sidekiq-pro'
require 'sidekiq/batch'


class FileCombinerWorker
  include Sidekiq::Worker
  sidekiq_options retry: false

  def perform(filepaths, destination_dir)
    return if cancelled?
    puts "Starting worker to combine #{filepaths.inspect}"

    file_names = filepaths.map { |filepath| Pathname.new(filepath).basename.to_s }
    result = filepaths.reduce(0) { |accumulator, filepath| accumulator += Integer(File.read(filepath)) }

    output_file = File.join(destination_dir, file_names.join("+"))
    raise "#{output_file} already exists" if File.exists?(output_file)

    puts "Finishing worker by touching #{output_file}"
    File.write(output_file, result)
  end

  private

  def cancelled?
    Sidekiq.redis {|c| c.exists("cancelled-#{jid}") } # Use c.exists? on Redis >= 4.2.0
  end
end

class DirectoryFinishedProcessing
  include Sidekiq::Worker
  sidekiq_options retry: false

  def on_success(status, options)
    return if cancelled?

    puts "on_success"
    puts status.inspect
    puts options.inspect
    system("tree #{options.fetch('base_dir')}")

    if FilesInDir.list(options.fetch('destination_dir')).length == 1
      result_filepath = FilesInDir.list(options.fetch('destination_dir')).first
      final_result = File.read(result_filepath)
      puts "Reached final result = #{final_result}"
      puts "Moving file to base path"
      FileUtils.mv(result_filepath, File.join(options.fetch('base_dir'), "result_is_#{final_result}"))
      puts "Cleaning up intermediate work products"
      FileUtils.rm_rf(File.join(options.fetch('base_dir'), "batch_1"))
      system("tree #{options.fetch('base_dir')}")
      puts "Done!"
    else
      puts "Enqueing another batch"
      next_source_dir = options.fetch('destination_dir') # the current destination becomes the source for the next batch
      next_iteration = options.fetch('iteration') + 1
      next_destination_dir = File.join(options.fetch('destination_dir'), "batch_#{next_iteration}")
      enque_batch_for_directory(options.fetch('base_dir'), next_source_dir, next_destination_dir, next_iteration)
    end
  end

  private

  def cancelled?
    Sidekiq.redis {|c| c.exists("cancelled-#{jid}") }
  end
end

class FilesInDir
  class << self
    def list(dir)
      Dir.entries(dir).reduce([]) do |files, entry|
        pathname = Pathname.new(File.join(dir, entry))
        files << pathname if pathname.file?
        files
      end
    end
  end
end


def enque_batch_for_directory(base_dir, source_dir, destination_dir, iteration)
  FileUtils.mkdir_p(destination_dir) unless Dir.exists?(destination_dir)

  batch = Sidekiq::Batch.new
  batch.description = destination_dir
  batch.on(:success, DirectoryFinishedProcessing, 'base_dir' => base_dir, 'destination_dir' => destination_dir, 'iteration' => iteration)
  batch.jobs do
    puts FilesInDir.list(source_dir).inspect
    puts FilesInDir.list(source_dir).count
    FilesInDir.list(source_dir).each_slice(2) do |files|
      FileCombinerWorker.perform_async(
        files,
        destination_dir
      )
    end
  end
  puts "Just started Batch #{batch.bid}"
end


base_dir = "batch_0"
FileUtils.rm_rf(base_dir)
FileUtils.mkdir_p(base_dir)
(1..20).each { |n| File.write(File.join(base_dir, n.to_s), n) }

source_dir = base_dir # special case for first iteration
iteration = 1
destination_dir = File.join(base_dir, "batch_#{iteration}")
enque_batch_for_directory(base_dir, source_dir, destination_dir, iteration)
