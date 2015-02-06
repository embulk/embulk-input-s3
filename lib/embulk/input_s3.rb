Embulk::JavaPlugin.register_input(
  :s3, "org.embulk.plugin.s3.S3FileInputPlugin",
  File.expand_path('../../../classpath', __FILE__))
