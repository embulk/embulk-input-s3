Embulk::JavaPlugin.register_input(
  :s3, "org.embulk.input.s3.S3FileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
