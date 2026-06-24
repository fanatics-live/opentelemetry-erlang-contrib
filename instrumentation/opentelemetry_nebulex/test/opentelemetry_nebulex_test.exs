defmodule OpentelemetryNebulexTest do
  use ExUnit.Case, async: false

  doctest OpentelemetryNebulex

  require OpenTelemetry.Tracer
  require OpenTelemetry.Span
  require Record

  for {name, spec} <- Record.extract_all(from_lib: "opentelemetry/include/otel_span.hrl") do
    Record.defrecord(name, spec)
  end

  for {name, spec} <- Record.extract_all(from_lib: "opentelemetry_api/include/opentelemetry.hrl") do
    Record.defrecord(name, spec)
  end

  defmodule Local do
    use Nebulex.Cache,
      otp_app: :opentelemetry_nebulex,
      adapter: Nebulex.Adapters.Local
  end

  defmodule Redis do
  end

  defmodule Partitioned do
    use Nebulex.Cache,
      otp_app: :opentelemetry_nebulex,
      adapter: Nebulex.Adapters.Partitioned
  end

  defmodule Multilevel do
    use Nebulex.Cache,
      otp_app: :opentelemetry_nebulex,
      adapter: Nebulex.Adapters.Multilevel

    defmodule L1 do
      use Nebulex.Cache,
        otp_app: :opentelemetry_nebulex,
        adapter: Nebulex.Adapters.Local
    end

    defmodule L2 do
      use Nebulex.Cache,
        otp_app: :opentelemetry_nebulex,
        adapter: Nebulex.Adapters.Partitioned
    end
  end

  setup do
    :otel_simple_processor.set_exporter(:otel_exporter_pid, self())

    OpenTelemetry.Tracer.start_span("test")

    on_exit(fn ->
      OpenTelemetry.Tracer.end_span()
    end)
  end

  test "local cache commands" do
    OpentelemetryNebulex.setup([:opentelemetry_nebulex_test, :local])

    start_supervised!(Local)

    # miss
    Local.get(:my_key)

    assert_receive {:span, span(name: "nebulex get", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.action": :miss,
             "nebulex.cache": OpentelemetryNebulexTest.Local,
             "db.system": :ets,
             "db.operation.name": :get,
             "db.query.text": "GET :my_key"
           } = :otel_attributes.map(attributes)

    # write
    Local.put(:my_key, 42)

    assert_receive {:span, span(name: "nebulex put", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.cache": OpentelemetryNebulexTest.Local,
             "db.system": :ets,
             "db.operation.name": :put,
             "db.query.text": "PUT :my_key"
           } = :otel_attributes.map(attributes)

    # hit
    Local.get(:my_key)

    assert_receive {:span, span(name: "nebulex get", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.action": :hit,
             "nebulex.cache": OpentelemetryNebulexTest.Local,
             "db.system": :ets,
             "db.operation.name": :get,
             "db.query.text": "GET :my_key"
           } = :otel_attributes.map(attributes)
  end

  test "partitioned cache commands" do
    OpentelemetryNebulex.setup([:opentelemetry_nebulex_test, :partitioned])
    OpentelemetryNebulex.setup([:opentelemetry_nebulex_test, :partitioned, :primary])

    start_supervised!(Partitioned)

    # miss
    Partitioned.get(:my_key)

    assert_receive {:span, span(name: "nebulex get", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.action": :miss,
             "db.system": :ets,
             "nebulex.cache": OpentelemetryNebulexTest.Partitioned.Primary
           } = :otel_attributes.map(attributes)

    assert_receive {:span, span(name: "nebulex get", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.action": :miss,
             "nebulex.cache": OpentelemetryNebulexTest.Partitioned,
             "nebulex.keyslot": Nebulex.Adapters.Partitioned
           } = :otel_attributes.map(attributes)

    # write
    Partitioned.put(:my_key, 42)

    assert_receive {:span, span(name: "nebulex put", kind: :internal, attributes: attributes)}

    assert %{
             "db.system": :ets,
             "nebulex.cache": OpentelemetryNebulexTest.Partitioned.Primary
           } = :otel_attributes.map(attributes)

    assert_receive {:span, span(name: "nebulex put", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.cache": OpentelemetryNebulexTest.Partitioned,
             "nebulex.keyslot": Nebulex.Adapters.Partitioned
           } = :otel_attributes.map(attributes)

    # hit
    Partitioned.get(:my_key)

    assert_receive {:span, span(name: "nebulex get", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.action": :hit,
             "db.system": :ets,
             "nebulex.cache": OpentelemetryNebulexTest.Partitioned.Primary
           } = :otel_attributes.map(attributes)

    assert_receive {:span, span(name: "nebulex get", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.action": :hit,
             "nebulex.cache": OpentelemetryNebulexTest.Partitioned,
             "nebulex.keyslot": Nebulex.Adapters.Partitioned
           } = :otel_attributes.map(attributes)
  end

  test "multi-level cache commands" do
    OpentelemetryNebulex.setup([:opentelemetry_nebulex_test, :multilevel])
    OpentelemetryNebulex.setup([:opentelemetry_nebulex_test, :multilevel, :l1])
    OpentelemetryNebulex.setup([:opentelemetry_nebulex_test, :multilevel, :l2])

    start_supervised!(
      {Multilevel,
       [
         levels: [
           {Multilevel.L1, []},
           {Multilevel.L2, []}
         ]
       ]}
    )

    # write
    Multilevel.put(:my_key, 42)

    assert_receive {:span, span(name: "nebulex put", kind: :internal, attributes: attributes)}

    assert %{
             "db.system": :ets,
             "nebulex.cache": OpentelemetryNebulexTest.Multilevel.L1
           } = :otel_attributes.map(attributes)

    assert_receive {:span, span(name: "nebulex put", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.cache": OpentelemetryNebulexTest.Multilevel.L2,
             "nebulex.keyslot": Nebulex.Adapters.Partitioned
           } = :otel_attributes.map(attributes)

    assert_receive {:span, span(name: "nebulex put", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.cache": OpentelemetryNebulexTest.Multilevel
           } = :otel_attributes.map(attributes)

    # hit
    Multilevel.get(:my_key)

    assert_receive {:span, span(name: "nebulex get", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.action": :hit,
             "db.system": :ets,
             "nebulex.cache": OpentelemetryNebulexTest.Multilevel.L1
           } = :otel_attributes.map(attributes)

    assert_receive {:span, span(name: "nebulex get", kind: :internal, attributes: attributes)}

    assert %{
             "nebulex.action": :hit,
             "nebulex.cache": OpentelemetryNebulexTest.Multilevel,
             "nebulex.model": :inclusive
           } = :otel_attributes.map(attributes)
  end

  test "batch cache commands include bounded db statements" do
    keys = Enum.to_list(1..12)

    metadata =
      command_metadata(:get_all, [keys], %{
        cache: Local,
        backend: :ets
      })

    attributes = run_command(metadata)

    assert %{
             "nebulex.cache": Local,
             "db.system": :ets,
             "db.operation.name": :get_all,
             "db.query.text": "MGET [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, ...]"
           } = attributes

    metadata =
      command_metadata(:put_all, [[one: 1, two: 2], :put_new], %{
        cache: Local,
        backend: :ets
      })

    attributes = run_command(metadata)

    assert %{
             "nebulex.cache": Local,
             "db.system": :ets,
             "db.operation.name": :put_all,
             "db.query.text": "MSETNX [:one, :two]"
           } = attributes
  end

  test "redis adapter metadata maps db system to redis" do
    metadata =
      command_metadata(:get_all, [[in: [:one, :two]]], %{
        cache: Redis,
        adapter: NebulexRedisAdapter
      })

    attributes = run_command(metadata)

    assert %{
             "nebulex.cache": Redis,
             "db.system": :redis,
             "db.operation.name": :get_all,
             "db.query.text": "MGET [:one, :two]"
           } = attributes
  end

  test "modern nebulex command metadata uses query op for db operation" do
    metadata = %{
      command: :execute,
      args: [%{op: :get_all, query: {:in, [:one, :two]}, select: {:key, :value}}, []],
      adapter_meta: %{
        cache: Redis,
        adapter: Nebulex.Adapters.Redis
      }
    }

    attributes = run_command(metadata)

    assert %{
             "nebulex.cache": Redis,
             "db.system": :redis,
             "db.operation.name": :get_all,
             "db.query.text": "MGET [:one, :two]"
           } = attributes
  end

  defp command_metadata(function_name, args, adapter_meta) do
    %{
      function_name: function_name,
      args: args,
      adapter_meta: adapter_meta
    }
  end

  defp run_command(metadata) do
    OpentelemetryNebulex.handle_command_start(nil, %{}, metadata, %{})
    OpentelemetryNebulex.handle_command_stop(nil, %{}, Map.put(metadata, :result, :ok), %{})

    assert_receive {:span,
                    span(name: "nebulex " <> _operation, kind: :internal, attributes: attrs)}

    :otel_attributes.map(attrs)
  end
end
