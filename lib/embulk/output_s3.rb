Embulk::JavaPlugin.register_output(
  :s3, "org.embulk.plugin.S3FileInputPlugin",
  File.expand_path(__FILE__, '../../../classpath'))
