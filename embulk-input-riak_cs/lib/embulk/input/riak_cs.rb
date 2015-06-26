Embulk::JavaPlugin.register_input(
  :riak_cs, "org.embulk.input.riak_cs.RiakCsFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
