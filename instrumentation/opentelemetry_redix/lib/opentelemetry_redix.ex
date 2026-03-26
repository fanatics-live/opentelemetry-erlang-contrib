defmodule OpentelemetryRedix do
  @moduledoc """
  OpentelemetryRedix uses [telemetry](https://hexdocs.pm/telemetry/) handlers to
  create `OpenTelemetry` spans.

  ## Usage

  In your application start:

      def start(_type, _args) do
        OpentelemetryRedix.setup()

        # ...
      end

  ## Options

  * `:db_namespace` - The Redis database index (e.g., `"0"`). Maps to the
    `db.namespace` semantic convention attribute. Omitted if not provided.
  """

  alias OpenTelemetry.SemConv.Incubating.DBAttributes
  alias OpenTelemetry.SemConv.ErrorAttributes
  alias OpenTelemetry.SemConv.ServerAttributes
  alias OpentelemetryRedix.Command
  alias OpentelemetryRedix.ConnectionTracker

  require OpenTelemetry.Tracer

  @default_redis_port 6379

  @typedoc "Setup options"
  @type opts :: [db_namespace: String.t()]

  @doc """
  Initializes and configures the telemetry handlers.
  """
  @spec setup(opts()) :: :ok
  def setup(opts \\ []) do
    config = %{db_namespace: Keyword.get(opts, :db_namespace)}

    :telemetry.attach(
      {__MODULE__, :pipeline_stop},
      [:redix, :pipeline, :stop],
      &__MODULE__.handle_pipeline_stop/4,
      config
    )
  end

  @doc false
  def handle_pipeline_stop(_event, measurements, meta, config) do
    duration = measurements.duration
    end_time = :opentelemetry.timestamp()
    start_time = end_time - duration

    {span_name, operation, batch_size} =
      case meta.commands do
        [[op | _args]] ->
          name = to_string(op)
          {name, name, nil}

        commands ->
          ops = Enum.map(commands, fn [op | _] -> to_string(op) end)
          {"PIPELINE " <> Enum.join(ops, " "), "PIPELINE", length(commands)}
      end

    statement = Enum.map_join(meta.commands, "\n", &Command.sanitize/1)

    connection = ConnectionTracker.get_connection(meta.connection)

    attributes =
      %{
        :"db.system.name" => "redis",
        DBAttributes.db_operation_name() => operation,
        DBAttributes.db_query_text() => statement
      }
      |> maybe_put(DBAttributes.db_namespace(), config[:db_namespace])
      |> maybe_put(DBAttributes.db_operation_batch_size(), batch_size)
      |> Map.merge(server_attributes(connection))
      |> Map.merge(redix_attributes(meta))

    parent_context =
      case OpentelemetryProcessPropagator.fetch_ctx(self()) do
        :undefined ->
          OpentelemetryProcessPropagator.fetch_parent_ctx(1, :"$callers")

        ctx ->
          ctx
      end

    parent_token =
      if parent_context != :undefined do
        OpenTelemetry.Ctx.attach(parent_context)
      else
        :undefined
      end

    s =
      OpenTelemetry.Tracer.start_span(span_name, %{
        start_time: start_time,
        kind: :client,
        attributes: attributes
      })

    if meta[:kind] == :error do
      OpenTelemetry.Span.set_status(s, OpenTelemetry.status(:error, format_error(meta.reason)))
      OpenTelemetry.Span.set_attributes(s, error_attributes(meta.reason))
    end

    OpenTelemetry.Span.end_span(s)

    if parent_token != :undefined do
      OpenTelemetry.Ctx.detach(parent_token)
    end
  end

  defp server_attributes(%{address: address}) when is_binary(address) do
    case String.split(address, ":") do
      [host, port_str] ->
        port = String.to_integer(port_str)
        attrs = %{ServerAttributes.server_address() => host}

        if port != @default_redis_port do
          Map.put(attrs, ServerAttributes.server_port(), port)
        else
          attrs
        end

      [host] ->
        %{ServerAttributes.server_address() => host}
    end
  end

  defp server_attributes(_), do: %{}

  defp redix_attributes(%{connection_name: nil}), do: %{}
  defp redix_attributes(%{connection_name: name}), do: %{"db.redix.connection_name": name}
  defp redix_attributes(_), do: %{}

  defp error_attributes(%{__exception__: true, message: message}) when is_binary(message) do
    status_code = message |> String.split(" ", parts: 2) |> List.first()

    %{
      :"db.response.status_code" => status_code,
      ErrorAttributes.error_type() => status_code
    }
  end

  defp error_attributes(reason) do
    %{ErrorAttributes.error_type() => inspect(reason)}
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp format_error(%{__exception__: true} = exception), do: Exception.message(exception)
  defp format_error(reason), do: inspect(reason)
end
