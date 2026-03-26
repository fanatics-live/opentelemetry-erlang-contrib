defmodule OpentelemetryRedixTest do
  use ExUnit.Case, async: false

  doctest OpentelemetryRedix

  alias OpenTelemetry.SemConv.Incubating.DBAttributes
  alias OpenTelemetry.SemConv.ServerAttributes

  require OpenTelemetry.Tracer
  require OpenTelemetry.Span
  require Record

  for {name, spec} <- Record.extract_all(from_lib: "opentelemetry/include/otel_span.hrl") do
    Record.defrecord(name, spec)
  end

  for {name, spec} <- Record.extract_all(from_lib: "opentelemetry_api/include/opentelemetry.hrl") do
    Record.defrecord(name, spec)
  end

  setup do
    :otel_simple_processor.set_exporter(:otel_exporter_pid, self())

    OpenTelemetry.Tracer.start_span("test")

    on_exit(fn ->
      :telemetry.detach({OpentelemetryRedix, :pipeline_stop})
      OpenTelemetry.Tracer.end_span()
    end)
  end

  test "records span on commands" do
    OpentelemetryRedix.setup()

    conn = start_supervised!({Redix, []})

    {:ok, "OK"} = Redix.command(conn, ["SET", "foo", "bar"])

    assert_receive {:span,
                    span(
                      name: "SET",
                      kind: :client,
                      attributes: attributes
                    )}

    attrs = :otel_attributes.map(attributes)

    assert attrs[DBAttributes.db_operation_name()] == "SET"
    assert attrs[DBAttributes.db_query_text()] == "SET foo ?"
    assert attrs[:"db.system.name"] == "redis"
    assert attrs[ServerAttributes.server_address()] == "localhost"
    refute Map.has_key?(attrs, ServerAttributes.server_port())
  end

  test "records span on pipelines" do
    OpentelemetryRedix.setup()

    conn = start_supervised!({Redix, []})

    {:ok, [_, _, _, "2"]} =
      Redix.pipeline(conn, [
        ["DEL", "counter"],
        ["INCR", "counter"],
        ["INCR", "counter"],
        ["GET", "counter"]
      ])

    assert_receive {:span,
                    span(
                      name: "PIPELINE DEL INCR INCR GET",
                      kind: :client,
                      attributes: attributes
                    )}

    attrs = :otel_attributes.map(attributes)

    assert attrs[DBAttributes.db_operation_name()] == "PIPELINE"

    assert attrs[DBAttributes.db_query_text()] ==
             "DEL counter\nINCR counter\nINCR counter\nGET counter"

    assert attrs[:"db.system.name"] == "redis"
    assert attrs[DBAttributes.db_operation_batch_size()] == 4
    assert attrs[ServerAttributes.server_address()] == "localhost"
  end

  test "records db.namespace when configured" do
    OpentelemetryRedix.setup(db_namespace: "1")

    conn = start_supervised!({Redix, []})

    {:ok, "OK"} = Redix.command(conn, ["SET", "foo", "bar"])

    assert_receive {:span,
                    span(
                      name: "SET",
                      kind: :client,
                      attributes: attributes
                    )}

    attrs = :otel_attributes.map(attributes)
    assert attrs[DBAttributes.db_namespace()] == "1"
  end

  test "omits db.namespace when not configured" do
    OpentelemetryRedix.setup()

    conn = start_supervised!({Redix, []})

    {:ok, "OK"} = Redix.command(conn, ["SET", "foo", "bar"])

    assert_receive {:span,
                    span(
                      name: "SET",
                      kind: :client,
                      attributes: attributes
                    )}

    attrs = :otel_attributes.map(attributes)
    refute Map.has_key?(attrs, DBAttributes.db_namespace())
  end
end
