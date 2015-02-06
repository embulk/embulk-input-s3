Embulk::JavaPlugin.register_output(
  :s3, "org.embulk.plugin.S3FileInputPlugin",
  File.expand_path('../../../classpath', __FILE__))
